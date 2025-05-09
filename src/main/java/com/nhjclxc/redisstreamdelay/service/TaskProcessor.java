package com.nhjclxc.redisstreamdelay.service;

public interface TaskProcessor {
    void process(String type, String content);
}
