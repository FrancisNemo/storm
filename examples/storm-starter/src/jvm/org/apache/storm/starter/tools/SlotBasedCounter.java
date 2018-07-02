/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.starter.tools;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * This class provides per-slot counts of the occurrences of objects.
 * <p/>
 * It can be used, for instance, as a building block for implementing sliding window counting of objects.
 *
 * @param <T> The type of those objects we want to count.
 */
public final class SlotBasedCounter<T> implements Serializable {

    private static final long serialVersionUID = 4858185737378394432L;

    /*对象为key, long[] 存储每个slot内统计的值*/
    private final Map<T, long[]> objToCounts = new HashMap<T, long[]>();

    /*窗口的长度*/
    private final int numSlots;

    public SlotBasedCounter(int numSlots) {
        if (numSlots <= 0) {
            throw new IllegalArgumentException("Number of slots must be greater than zero (you requested " + numSlots + ")");
        }
        this.numSlots = numSlots;
    }

    public void incrementCount(T obj, int slot) {
        long[] counts = objToCounts.get(obj);
        if (counts == null) {
            counts = new long[this.numSlots];
            objToCounts.put(obj, counts);
        }
        counts[slot]++;
    }

    public long getCount(T obj, int slot) {
        long[] counts = objToCounts.get(obj);
        if (counts == null) {
            return 0;
        } else {
            return counts[slot];
        }
    }

    public Map<T, Long> getCounts() {
        Map<T, Long> result = new HashMap<T, Long>();
        objToCounts.forEach((k,v)->{result.put(k, computeTotalCount(k));});
//        for (T obj : objToCounts.keySet()) {
//            result.put(obj, computeTotalCount(obj));
//        }
        return result;
    }

    private long computeTotalCount(T obj) {
        return Arrays.stream(objToCounts.get(obj)).count();
    }

    /**
     * Reset the slot count of any tracked objects to zero for the given slot.
     *
     * @param slot
     */
    @Deprecated
    public void wipeSlot(int slot) {
        for (T obj : objToCounts.keySet()) {
            resetSlotCountToZero(obj, slot);
        }
    }

    private void resetSlotCountToZero(T obj, int slot) {
        long[] counts = objToCounts.get(obj);
        counts[slot] = 0;
    }

    /*lambda 清理整列*/
    public void wipeSlotForEach(int slot) {
        objToCounts.forEach((k,v)->{
            v[slot] = 0;
            }
        );
    }

    private boolean shouldBeRemovedFromCounter(T obj) {
        return computeTotalCount(obj) == 0;
    }

    /**
     * Remove any object from the counter whose total count is zero (to free up memory).
     */
    @Deprecated
    public void wipeZeros() {
        for (Iterator<Map.Entry<T, long[]>> it = objToCounts.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<T, long[]> entry = it.next();
            if (shouldBeRemovedFromCounter(entry.getKey())) {
                it.remove();
            }
        }
    }

    /*lambda 清理整行*/
    public void wipeZerosForEach() {
        objToCounts.forEach((k,v)->{
            if(computeTotalCount(k) == 0){
                objToCounts.remove(k);
            }
        });
    }

}
