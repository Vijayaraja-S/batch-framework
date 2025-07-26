package com.p3.batchframework.persistence.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.persistence.*;
import java.io.Serializable;
import java.util.*;
import lombok.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Entity
@Table(name = "background_job")
public class BackgroundJobEntity extends BaseEntity implements Serializable {
  private String name;

  private String description;

  private String status;

  private String type;

  @Column(name = "jobInput")
  @JdbcTypeCode(SqlTypes.BINARY)
  @Lob
  private byte[] jobInput;

  @Temporal(TemporalType.TIMESTAMP)
  @Column(name = "startTime")
  private Date startTime;

  @Column(name = "message")
  @JdbcTypeCode(SqlTypes.BINARY)
  @Lob
  private byte[] message;

  @Column
  @JdbcTypeCode(SqlTypes.BINARY)
  @Lob
  private byte[] downloadMessage;

  private String downloadStatus;

  @JsonIgnore
  @Column(name = "file")
  @JdbcTypeCode(SqlTypes.BINARY)
  @Lob
  private byte[] outputFile;
}
