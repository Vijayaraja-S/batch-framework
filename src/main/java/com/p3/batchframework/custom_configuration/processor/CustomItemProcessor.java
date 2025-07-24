package com.p3.batchframework.custom_configuration.processor;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStream;

public interface CustomItemProcessor<I, O> extends ItemProcessor<I, O>, ItemStream {}
