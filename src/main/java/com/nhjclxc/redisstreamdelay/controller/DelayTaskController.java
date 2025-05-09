package com.nhjclxc.redisstreamdelay.controller;

import com.nhjclxc.redisstreamdelay.model.DelayTaskDTO;
import com.nhjclxc.redisstreamdelay.service.DelayTaskService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/delay")
public class DelayTaskController {

    @Autowired
    private DelayTaskService delayTaskService;

    @PostMapping("/add")
    public String addTask(@RequestBody DelayTaskDTO taskDTO) {
        delayTaskService.addTask(taskDTO);
        return "Task added";
    }
}
