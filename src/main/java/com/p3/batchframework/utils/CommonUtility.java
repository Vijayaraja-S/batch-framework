package com.p3.batchframework.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.io.*;
import java.math.BigInteger;
import java.util.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class CommonUtility {

  public String convertObjectToJSON(Object obj) {
    ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
    String json = StringUtils.EMPTY;
    try {
      json = ow.writeValueAsString(obj);
    } catch (Exception e) {
      extracted(e);
    }
    return json;
  }

  public Object convertJSONStringToObject(String jsonString, Class<?> outCLass) {
    ObjectMapper mapper = new ObjectMapper();
    Object obj = null;
    try {
      obj = mapper.readValue(jsonString, outCLass);
    } catch (Exception e) {
      extracted(e);
    }
    return obj;
  }

  private static void extracted(Exception e) {
    log.error("Exception while converting mapperbean to JSON string {}", e.getMessage());
  }

  public BigInteger convertLongToBigInteger(Long data) {
    return BigInteger.valueOf(data);
  }

  public String convertLongToString(Long data) {
    return String.valueOf(data);
  }

  public Long convertStringToLong(String data) {
    return Long.parseLong(data);
  }

  public BigInteger convertStringToBigInteger(String data) {
    return new BigInteger(data);
  }

  public byte[] convertMapToByteArray(Map<String, ?> map, boolean usedSafeCopy) {
    ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(byteOut)) {
      out.writeObject(map);
      out.flush();
    } catch (ConcurrentModificationException e) {
      if (usedSafeCopy) {
        log.error(
            "Still encountering ConcurrentModificationException after safe copy: {}",
            e.getMessage(),
            e);
        throw e;
      }
      Map<String, Object> safeCopy = new HashMap<>();
      log.warn("Retrying to serialize map with safe copy");
      if (map != null) {
        safeCopy = safeCopy(map);
      }
      return convertMapToByteArray(safeCopy, true);
    } catch (Exception exception) {
      log.error("Error serializing map: {}", exception.getMessage(), exception);
    }
    return byteOut.toByteArray();
  }

  public Map<String, Object> safeCopy(Map<String, ?> map) {
    Map<String, Object> safeCopy = new HashMap<>();
    ObjectMapper objectMapper = new ObjectMapper();
    synchronized (map) {
      for (Map.Entry<String, ?> entry : map.entrySet()) {
        String key = entry.getKey();
        Object value = entry.getValue();

        if (value == null) {
          safeCopy.put(key, null);
          continue;
        }

        if (value instanceof ConcurrentHashMap) {
          @SuppressWarnings("unchecked")
          ConcurrentHashMap<String, Object> originalSubMap =
              (ConcurrentHashMap<String, Object>) value;
          HashMap<String, Object> subMapCopy = new HashMap<>();
          synchronized (originalSubMap) {
            for (Map.Entry<String, Object> subEntry : originalSubMap.entrySet()) {
              subMapCopy.put(subEntry.getKey(), makeDeepCopy(subEntry.getValue()));
            }
          }
          safeCopy.put(key, objectMapper.convertValue(subMapCopy, value.getClass()));
        } else if (value instanceof Map) {
          @SuppressWarnings("unchecked")
          Map<String, Object> originalSubMap = (Map<String, Object>) value;
          HashMap<String, Object> subMapCopy = new HashMap<>();
          synchronized (originalSubMap) {
            for (Map.Entry<String, Object> subEntry : originalSubMap.entrySet()) {
              subMapCopy.put(subEntry.getKey(), makeDeepCopy(subEntry.getValue()));
            }
          }
          safeCopy.put(key, objectMapper.convertValue(subMapCopy, value.getClass()));
        } else if (value instanceof List) {
          List<?> originalList = (List<?>) value;
          List<Object> listCopy = new ArrayList<>(originalList.size());

          List<?> listSnapshot;
          synchronized (originalList) {
            listSnapshot = new ArrayList<>(originalList);
          }

          for (Object item : listSnapshot) {
            listCopy.add(makeDeepCopy(item));
          }

          safeCopy.put(key, objectMapper.convertValue(listCopy, value.getClass()));
        } else if (value instanceof Set) {
          Set<?> originalSet = (Set<?>) value;
          HashSet<Object> setCopy = new HashSet<>();

          Set<?> setSnapshot;
          synchronized (originalSet) {
            setSnapshot = new HashSet<>(originalSet);
          }
          for (Object item : setSnapshot) {
            setCopy.add(makeDeepCopy(item));
          }
          safeCopy.put(key, objectMapper.convertValue(setCopy, value.getClass()));
        } else {
          safeCopy.put(key, objectMapper.convertValue(value, value.getClass()));
        }
      }
    }
    return safeCopy;
  }

  /** Helper method to create deep copies of objects */
  private Object makeDeepCopy(Object obj) {
    ObjectMapper oMapper = new ObjectMapper();
    if (obj == null) {
      return null;
    }
    if (obj instanceof ConcurrentHashMap) {
      @SuppressWarnings("unchecked")
      ConcurrentHashMap<String, Object> original = (ConcurrentHashMap<String, Object>) obj;
      HashMap<String, Object> copy = new HashMap<>();

      synchronized (original) {
        for (Map.Entry<String, Object> entry : original.entrySet()) {
          copy.put(entry.getKey(), makeDeepCopy(entry.getValue()));
        }
      }
      return oMapper.convertValue(copy, obj.getClass());
    } else if (obj instanceof Map) {
      @SuppressWarnings("unchecked")
      Map<String, Object> original = (Map<String, Object>) obj;
      HashMap<String, Object> copy = new HashMap<>();

      synchronized (original) {
        for (Map.Entry<String, Object> entry : original.entrySet()) {
          copy.put(entry.getKey(), makeDeepCopy(entry.getValue()));
        }
      }
      return oMapper.convertValue(copy, obj.getClass());
    } else if (obj instanceof List) {
      List<?> original = (List<?>) obj;
      ArrayList<Object> copy = new ArrayList<>(original.size());
      List<?> listSnapshot;
      synchronized (original) {
        listSnapshot = new ArrayList<>(original);
      }
      for (Object item : listSnapshot) {
        copy.add(makeDeepCopy(item));
      }

      return oMapper.convertValue(copy, obj.getClass());
    } else if (obj instanceof Set) {
      Set<?> original = (Set<?>) obj;
      HashSet<Object> copy = new HashSet<>();

      Set<?> setSnapshot;
      synchronized (original) {
        setSnapshot = new HashSet<>(original);
      }
      for (Object item : setSnapshot) {
        copy.add(makeDeepCopy(item));
      }

      return oMapper.convertValue(copy, obj.getClass());
    }
    return oMapper.convertValue(obj, obj.getClass());
  }

  @SuppressWarnings("unchecked")
  public Map<String, ?> convertByteArrayToMap(byte[] bytes) {
    ByteArrayInputStream byteIn = new ByteArrayInputStream(bytes);
    ObjectInputStream in = null;
    Map<String, ?> map;
    try {
      in = new ObjectInputStream(byteIn);
      map = (Map<String, ?>) in.readObject();
    } catch (Exception e) {
      log.error("Unexpected error Occurred: {}", e.getMessage());
      log.error("Error deserializing map", e);
      map = new HashMap<>();
    } finally {
      if (in != null) {
        try {
          in.close();
        } catch (IOException e) {
          log.error("Error closing stream", e);
        }
      }
    }
    return map;
  }
}
