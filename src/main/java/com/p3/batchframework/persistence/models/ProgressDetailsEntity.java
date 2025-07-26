package com.p3.batchframework.persistence.models;

import jakarta.persistence.*;
import java.util.List;
import lombok.*;
import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;

@AllArgsConstructor(access = AccessLevel.PUBLIC)
@Builder
@NoArgsConstructor(access = AccessLevel.PUBLIC)
@Table(name = "progress_details")
@Entity
public class ProgressDetailsEntity extends BaseEntity {
  @Column(columnDefinition = "text")
  private String level;

  @Column(columnDefinition = "text")
  private String name;

  private Boolean estimating;
  private Boolean showHud;
  private Long recordProcessed;
  private Long recordsRead;
  private Long avgThroughput;
  private Long totalRecords;
  private float progress;
  private Long estimatedTime;
  private Long elapsedTime;

  @OneToMany(cascade = CascadeType.ALL, fetch = FetchType.EAGER)
  @Fetch(FetchMode.SELECT)
  @JoinColumn(name = "step_progress_details_id", referencedColumnName = "id")
  private List<ProgressDetailsEntity> subProgress;

  private Long combinedStepSize;
}
