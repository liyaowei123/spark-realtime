package edu.lyw.publisherrealtime.service;

import edu.lyw.publisherrealtime.bean.NameValue;

import java.util.List;
import java.util.Map;

public interface PublisherService {
    Map<String, Object> doDauRealtime(String td);

    List<NameValue> doStatsByItem(String itemName, String date, String t);

    Map<String, Object> doDetailByItem(String date, String itemName, Integer pageNo, Integer pageSize);
}
