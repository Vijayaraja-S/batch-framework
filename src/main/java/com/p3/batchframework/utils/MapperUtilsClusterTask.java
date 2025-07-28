package com.p3.batchframework.utils;

import java.io.*;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.modelmapper.ModelMapper;
import org.modelmapper.convention.MatchingStrategies;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class MapperUtilsClusterTask {
  ModelMapper modelMapper = new ModelMapper();

  public MapperUtilsClusterTask() {
    modelMapper = new ModelMapper();
    modelMapper.getConfiguration().setMatchingStrategy(MatchingStrategies.STRICT);
  }

  public <T, D> D map(T entity, Class<D> outClass) {
    return modelMapper.map(entity, outClass);
  }

  public <T, D> List<D> map(Collection<T> entityList, Class<D> outCLass) {
    return entityList.stream().map(entity -> map(entity, outCLass)).collect(Collectors.toList());
  }

  public Map<String, ?> convertByteArrayToMap(byte[] bytes) {
    ByteArrayInputStream byteIn = new ByteArrayInputStream(bytes);
    ObjectInputStream in = null;
    Map<String, ?> map = null;
    try {
      in = new ObjectInputStream(byteIn);
      map = (Map<String, ?>) in.readObject();
    } catch (Exception e) {
      log.error("Unexpected error Occurred: {}", e.getMessage());
    }
    return map;
  }

  public byte[] convertMapToByteArray(Map<String, ?> map) {
    ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
    ObjectOutputStream out = null;
    try {
      out = new ObjectOutputStream(byteOut);
      out.writeObject(map);
      return byteOut.toByteArray();
    } catch (IOException e) {

      return null;
    }
  }
}
