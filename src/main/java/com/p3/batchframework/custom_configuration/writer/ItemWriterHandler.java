package com.p3.batchframework.custom_configuration.writer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component("ItemWriterHandler")
public class ItemWriterHandler<T> extends AbstractItemWriterHandler<T> {}
