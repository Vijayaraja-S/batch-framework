package com.p3.batchframework.custom_configuration.reader;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component("ItemReaderHandler")
public class ItemReaderHandler<T> extends AbstractItemReaderHandler<T> {}
