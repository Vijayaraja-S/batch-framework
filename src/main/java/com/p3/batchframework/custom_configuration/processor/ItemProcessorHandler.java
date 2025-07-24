package com.p3.batchframework.custom_configuration.processor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component("ItemProcessorHandler")
public class ItemProcessorHandler<I,O> extends  AbstractItemProcessorHandler<I,O>{}
