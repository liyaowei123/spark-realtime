package edu.lyw.publisherrealtime.mapper;

import edu.lyw.publisherrealtime.bean.NameValue;

import java.util.List;
import java.util.Map;

public interface PublisherMapper {
    Map<String, Object> searchDau(String td);

    List<NameValue> searchStatsByItem(String itemName, String date, String t);

    Map<String, Object> searchDetailByItem(String date, String itemName, Integer pageNo, Integer pageSize);
}
