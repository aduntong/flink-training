/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.training.solutions.longrides;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.sources.TaxiFareGenerator;
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator;
import org.apache.flink.training.exercises.common.utils.ExerciseBase;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.math.BigDecimal;

/**
 * Solution to the "Long Ride Alerts" exercise of the Flink training in the docs.
 *
 * <p>The goal for this exercise is to emit START events for taxi rides that have not been matched
 * by an END event during the first 2 hours of the ride.
 * https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/learn-flink/event_driven/
 * <p>
 * 对过长的 ride session 的处理，注册一个 timer，清理状态
 */
public class LongRidesSolution extends ExerciseBase {
    private static OutputTag<TaxiRide> lateRideOutput = new OutputTag<>("lateRide", TypeInformation.of(TaxiRide.class));
    private static OutputTag<TaxiFare> recordedFareOutput = new OutputTag<>("recordedFare", TypeInformation.of(TaxiFare.class));
    private static OutputTag<TaxiFare> lateFareOutput = new OutputTag<>("lateFare", TypeInformation.of(TaxiFare.class));

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(ExerciseBase.parallelism);

        // start the data generator
        DataStream<TaxiRide> rides = env.addSource(rideSourceOrTest(new TaxiRideGenerator()));
        DataStream<TaxiFare> fare = env.addSource(fareSourceOrTest(new TaxiFareGenerator()));

        DataStream<TaxiRide> longRides = rides
                // not possible in real world scenario
                .keyBy((TaxiRide ride) -> ride.rideId)
                .process(new MatchFunction());

        SingleOutputStreamOperator<TaxiRide> hourlyRideStream = rides.keyBy(r -> r.driverId)
                .process(new HourlyRidesFunction());
        hourlyRideStream
                .getSideOutput(lateRideOutput)
                .print("lateRideOutput");

        SingleOutputStreamOperator<Tuple4<Long, Long, BigDecimal, String>> hourlyFareTipSumsStream = fare.keyBy(r -> r.driverId)
                .process(new HourlyTipsFunction());
        hourlyFareTipSumsStream.print("hourlyFareTipSumsStream");

        hourlyFareTipSumsStream
                .getSideOutput(lateFareOutput)
                .print("lateFareOutput");
        hourlyFareTipSumsStream
                .getSideOutput(recordedFareOutput)
                .print("recordedFareOutput");

        printOrTest(longRides);

        env.execute("Long Taxi Rides");
    }

    /**
     * key by driver id
     */
    private static class HourlyRidesFunction extends KeyedProcessFunction<Long, TaxiRide, TaxiRide> {
        // 窗口尺寸
        private static Long durationMsec = 60 * 60 * 1000L;
        // 时间范围内开始的 ride 列表
        private ListState<TaxiRide> rideListState;
        // 时间范围内结束的 ride 列表, key 为开始时间 timestamp, 没用
        private MapState<Long, TaxiRide> rideMapState;
//        OutputTag<TaxiRide> lateRideOutput = new OutputTag<>("lateRide", TypeInformation.of(TaxiRide.class));

        @Override
        public void open(Configuration config) {
            ListStateDescriptor<TaxiRide> listStateDescriptor = new ListStateDescriptor<>("hourly ride event", TaxiRide.class);
            MapStateDescriptor<Long, TaxiRide> mapStateDescriptor = new MapStateDescriptor<>("hourly mapped ride event", Long.class, TaxiRide.class);
            rideListState = getRuntimeContext().getListState(listStateDescriptor);
            rideMapState = getRuntimeContext().getMapState(mapStateDescriptor);
        }

        @Override
        public void processElement(TaxiRide value, KeyedProcessFunction<Long, TaxiRide, TaxiRide>.Context ctx, Collector<TaxiRide> out) throws Exception {
            long eventTime = value.getEventTime();
            TimerService timerService = ctx.timerService();

            if (eventTime <= timerService.currentWatermark()) {
                ctx.output(lateRideOutput, value);
            } else {
                // Round up eventTime to the end of the window containing this event.
                long endOfWindow = (eventTime - (eventTime % durationMsec) + durationMsec - 1);

                // Schedule a callback for when the window has been completed.
                timerService.registerEventTimeTimer(endOfWindow);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<TaxiRide> out) throws Exception {
            // go to side output
            for (TaxiRide ride : rideListState.get()) {
                out.collect(ride);
            }
            rideListState.clear();
            // 这个 map 没啥用
            rideMapState.clear();
        }
    }

    /**
     * 按小时统计 tips
     * <p>
     * Long, driver id
     * Long, end window, ts
     * BigDecimal, sum of tips
     * String,    final / processing
     */
    private static class HourlyTipsFunction extends KeyedProcessFunction<Long, TaxiFare, Tuple4<Long, Long, BigDecimal, String>> {
        // 窗口尺寸
        private static Long durationMsec = 60 * 60 * 1000L;
        // Keyed, managed state, with an entry for each window, keyed by the window's end time.
        // There is a separate MapState object for each driver.
        private transient MapState<Long, BigDecimal> sumOfTips;

        // 时间范围内开始的 ride 列表
        private ListState<TaxiFare> fareListState;
        // 时间范围内结束的 ride 列表, key 为开始时间 timestamp
        private MapState<Long, TaxiFare> fareMapState;


        @Override
        public void open(Configuration config) {
            ListStateDescriptor<TaxiFare> listStateDescriptor = new ListStateDescriptor<>("hourly ride event", TaxiFare.class);
            MapStateDescriptor<Long, TaxiFare> mapStateDescriptor = new MapStateDescriptor<>("hourly mapped ride event", Long.class, TaxiFare.class);
            fareListState = getRuntimeContext().getListState(listStateDescriptor);
            fareMapState = getRuntimeContext().getMapState(mapStateDescriptor);

            MapStateDescriptor<Long, BigDecimal> sumDesc = new MapStateDescriptor<>("sumOfTips", Long.class, BigDecimal.class);
            sumOfTips = getRuntimeContext().getMapState(sumDesc);
        }

        @Override
        public void processElement(TaxiFare value, KeyedProcessFunction<Long, TaxiFare, Tuple4<Long, Long, BigDecimal, String>>.Context ctx, Collector<Tuple4<Long, Long, BigDecimal, String>> out) throws Exception {
            long eventTime = value.getEventTime();
            TimerService timerService = ctx.timerService();

            if (eventTime <= timerService.currentWatermark()) {
                // This event is late; its window has already been triggered.
                // go to side output or update historical data
                ctx.output(lateFareOutput, value);
            } else {
                // Round up eventTime to the end of the window containing this event.
                long endOfWindow = (eventTime - (eventTime % durationMsec) + durationMsec - 1);

                // Schedule a callback for when the window has been completed.
                timerService.registerEventTimeTimer(endOfWindow);

                // Add this fare's tip to the running total for that window.
                BigDecimal sum = sumOfTips.get(endOfWindow);
                if (sum == null) {
                    sum = BigDecimal.ZERO;
                }
                sum = sum.add(value.tip);
                sumOfTips.put(endOfWindow, sum);
                out.collect(Tuple4.of(ctx.getCurrentKey(), System.currentTimeMillis(), sum, "processing"));
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<Tuple4<Long, Long, BigDecimal, String>> out) throws Exception {
            // go to side output
            for (TaxiFare fare : fareListState.get()) {
                context.output(recordedFareOutput, fare);
            }
            fareListState.clear();
            // 这个 map 没啥用
            fareMapState.clear();

            // if we get here, we know that the ride started two hours ago, and the END hasn't been processed
            out.collect(Tuple4.of(context.getCurrentKey(), timestamp, sumOfTips.get(timestamp), "final"));
            sumOfTips.clear();
        }
    }

    /***
     * 强制结束超长的 ride
     * key ride id
     */
    private static class MatchFunction extends KeyedProcessFunction<Long, TaxiRide, TaxiRide> {

        private ValueState<TaxiRide> rideState;

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<TaxiRide> stateDescriptor = new ValueStateDescriptor<>("ride event", TaxiRide.class);

            rideState = getRuntimeContext().getState(stateDescriptor);

        }

        @Override
        public void processElement(TaxiRide ride, Context context, Collector<TaxiRide> out) throws Exception {
            TaxiRide previousRideEvent = rideState.value();

            if (previousRideEvent == null) {
                rideState.update(ride);
                if (ride.isStart) {
                    context.timerService().registerEventTimeTimer(getTimerTime(ride));
                }
            } else {
                if (!ride.isStart) {
                    // it's an END event, so event saved was the START event and has a timer
                    // the timer hasn't fired yet, and we can safely kill the timer
                    context.timerService().deleteEventTimeTimer(getTimerTime(previousRideEvent));
                }
                // both events have now been seen, we can clear the state
                rideState.clear();
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<TaxiRide> out) throws Exception {

            // if we get here, we know that the ride started two hours ago, and the END hasn't been processed
            out.collect(rideState.value());
            rideState.clear();
        }

        private long getTimerTime(TaxiRide ride) {
            return ride.startTime.plusSeconds(120 * 60).toEpochMilli();
        }
    }

}
