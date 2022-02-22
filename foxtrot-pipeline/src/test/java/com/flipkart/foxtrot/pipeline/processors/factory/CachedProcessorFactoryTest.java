package com.flipkart.foxtrot.pipeline.processors.factory;

import static org.junit.Assert.*;

import com.flipkart.foxtrot.pipeline.processors.Processor;
import com.flipkart.foxtrot.pipeline.processors.ProcessorCreationException;
import com.flipkart.foxtrot.pipeline.processors.ProcessorDefinition;
import com.flipkart.foxtrot.pipeline.processors.TestProcessor;
import com.flipkart.foxtrot.pipeline.processors.TestProcessorDefinition;
import com.flipkart.foxtrot.pipeline.processors.string.StringLowerProcessorDefinition;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.UncheckedExecutionException;
import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicInteger;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import lombok.val;
import org.junit.Assert;
import org.junit.Test;

public class CachedProcessorFactoryTest {
    private AtomicInteger counterCalled;

    @Test
    public void checkCachedCreate() throws ProcessorCreationException {
        counterCalled = new AtomicInteger(0);

        val underTest = new CachedProcessorFactory(new ProcessorFactory() {
            @Override
            public Processor create(ProcessorDefinition process) {
                counterCalled.incrementAndGet();
                if (process.getType().equals("TEST"))
                    return new TestProcessor();
                return null;
            }
        }, CacheBuilder.newBuilder().initialCapacity(10).expireAfterWrite(1000, TimeUnit.MINUTES));
        val createdProcessor = underTest.create(new TestProcessorDefinition());
        Assert.assertTrue(createdProcessor instanceof TestProcessor);
        Assert.assertEquals(1, counterCalled.get());

        val createdProcessorSecondTime = underTest.create(new TestProcessorDefinition());
        Assert.assertTrue(createdProcessorSecondTime instanceof TestProcessor);
        Assert.assertEquals(1, counterCalled.get());

    }

    @Test
    public void checkCachedCreateNull() throws ExecutionException {
        counterCalled = new AtomicInteger(0);

        val underTest = new CachedProcessorFactory(new ProcessorFactory() {
            @Override
            public Processor create(ProcessorDefinition process) {
                counterCalled.incrementAndGet();
                if (process.getType().equals("TEST"))
                    return new TestProcessor();
                return null;
            }
        }, CacheBuilder.newBuilder().initialCapacity(10).expireAfterWrite(1000, TimeUnit.MINUTES));
        try {
            val createdProcessor = underTest.create(new StringLowerProcessorDefinition());
            fail("Null return would cause Execution exception");

        } catch (ProcessorCreationException e) {
        }

    }

    @Test
    public void checkCachedCreateException() throws ExecutionException {
        counterCalled = new AtomicInteger(0);

        val underTest = new CachedProcessorFactory(new ProcessorFactory() {
            @Override
            public Processor create(ProcessorDefinition process) {
                throw new IllegalArgumentException("No processor def registered");
            }
        }, CacheBuilder.newBuilder().initialCapacity(10).expireAfterWrite(1000, TimeUnit.MINUTES));
        try {
            val createdProcessor = underTest.create(new StringLowerProcessorDefinition());
            fail("Exception raised would cause Execution exception");

        } catch (ProcessorCreationException e) {
            assertTrue(e.getCause().getCause() instanceof IllegalArgumentException);
        }

    }
}