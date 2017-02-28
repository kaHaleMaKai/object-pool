package com.github.kahalemakai.whirlpool;

import lombok.val;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.junit.Assert.*;

public class BufferEntryTest {
    private Field idField, valueField;
    private Method mAdd, mTake;

    @Test
    public void add() throws Exception {
        val requestId = 23L;
        val newValue = 1231;
        final BufferEntry<Integer> entry = BufferEntry.ofId(requestId);
        assertEquals(-requestId, idField.get(entry));
        assertTrue((boolean) mAdd.invoke(entry, requestId, newValue));
        assertEquals(requestId, idField.get(entry));
    }

    @Test
    public void addFails() throws Exception {
        val requestId = 23L;
        val newValue = 1231;
        final BufferEntry<Integer> entry = BufferEntry.ofId(requestId);
        assertFalse((boolean) mAdd.invoke(entry, newValue, requestId+1));
    }

    @Test
    public void take() throws Exception {
        val requestId = 23L;
        val newValue = 1231;
        val bufferSize = 92342;
        final BufferEntry<Integer> entry = BufferEntry.ofId(requestId);
        assertTrue((boolean) mAdd.invoke(entry, requestId, newValue));
        assertEquals(newValue, mTake.invoke(entry, requestId, bufferSize));
        assertEquals(-(requestId + bufferSize), idField.get(entry));
        assertNull(valueField.get(entry));
    }

    @Test
    public void takeFails() throws Exception {
        val requestId = 23L;
        val newValue = 34234;
        val bufferSize = 92342;
        final BufferEntry<Integer> entry = BufferEntry.ofId(requestId);
        assertEquals(null, mTake.invoke(entry, requestId, bufferSize));
        assertTrue((boolean) mAdd.invoke(entry, requestId, newValue));
        assertEquals(null, mTake.invoke(entry, requestId+1, bufferSize));
    }

    @Before
    public void setUp() throws Exception {
        idField = BufferEntry.class.getDeclaredField("id");
        valueField = BufferEntry.class.getDeclaredField("value");
        mAdd = BufferEntry.class.getDeclaredMethod("add", long.class, Object.class);
        mTake = BufferEntry.class.getDeclaredMethod("take", long.class, int.class);
        idField.setAccessible(true);
        valueField.setAccessible(true);
        mAdd.setAccessible(true);
        mTake.setAccessible(true);
    }
}