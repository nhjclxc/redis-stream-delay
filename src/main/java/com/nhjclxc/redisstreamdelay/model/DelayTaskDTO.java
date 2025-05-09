package com.nhjclxc.redisstreamdelay.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DelayTaskDTO {
    private String taskId;
    private String type;
    private String content;
    private long delayMillis;
}
