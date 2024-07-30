/*
 * Copyright 2023 Greptime Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.greptime.rpc;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author jiachun.fjc
 */
public class ContextTest {
    @Test
    public void newDefaultShouldReturnEmptyContextTest() {
        Context context = Context.newDefault();
        Assert.assertTrue(context.entrySet().isEmpty());
    }

    @Test
    public void ofShouldReturnContextWithOneEntryTest() {
        Context context = Context.of("key", "value");
        Assert.assertEquals("value", context.get("key"));
    }

    @Test
    public void hintShouldReturnContextWithHintEntryTest() {
        Context context = Context.hint("key", "value");
        Assert.assertEquals("value", context.get("x-greptime-hint:key"));
    }

    @Test
    public void withShouldAddEntryToContextTest() {
        Context context = Context.newDefault();
        context.with("key", "value");
        Assert.assertEquals("value", context.get("key"));
    }

    @Test
    public void withHintShouldAddHintEntryToContextTest() {
        Context context = Context.newDefault();
        context.withHint("key", "value").with("key2", "value");
        Assert.assertEquals("value", context.get("x-greptime-hint:key"));
        Assert.assertEquals("value", context.get("key2"));
    }

    @Test
    public void getShouldReturnNullForNonExistingKeyTest() {
        Context context = Context.newDefault();
        Assert.assertNull(context.get("key"));
    }

    @Test
    public void removeShouldRemoveEntryFromContextTest() {
        Context context = Context.of("key", "value");
        String value = context.remove("key");
        Assert.assertNull(context.get("key"));
        Assert.assertEquals("value", value);
    }

    @Test
    public void getOrDefaultShouldReturnDefaultValueForNonExistingKeyTest() {
        Context context = Context.newDefault();
        Assert.assertEquals("default", context.getOrDefault("key", "default"));
    }

    @Test
    public void clearShouldRemoveAllEntriesFromContextTest() {
        Context context = Context.of("key", "value");
        context.clear();
        Assert.assertTrue(context.entrySet().isEmpty());
    }

    @Test
    public void copyShouldReturnIdenticalContextTest() {
        Context context = Context.of("key", "value");
        Context copy = context.copy();
        Assert.assertEquals(context.get("key").toString(), copy.get("key").toString());
    }
}
