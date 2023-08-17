package edu.lyw.publisherrealtime.service.impl;

import edu.lyw.publisherrealtime.bean.NameValue;
import edu.lyw.publisherrealtime.mapper.PublisherMapper;
import edu.lyw.publisherrealtime.service.PublisherService;
import org.apache.lucene.search.NamedMatches;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    PublisherMapper publisherMapper;

    @Override
    public Map<String, Object> doDetailByItem(String date, String itemName, Integer pageNo, Integer pageSize) {
        Integer from = (pageNo - 1) * pageSize;
        Map<String, Object> results = publisherMapper.searchDetailByItem(date, itemName, from, pageSize);
        return results;
    }

    @Override
    public List<NameValue> doStatsByItem(String itemName, String date, String t) {
        List<NameValue> list = publisherMapper.searchStatsByItem(itemName, date, typeToField(t));
        return transformResults(list, t);
    }

    @Override
    public Map<String, Object> doDauRealtime(String td) {
        System.out.println("service...");
        Map<String, Object> results = publisherMapper.searchDau(td);
        return results;
    }

    public String typeToField(String type) {
        if (type.equals("age")) {
            return "user_age";
        } else if (type.equals("gender")) {
            return "user_gender";
        } else {
            return null;
        }
    }

    public List<NameValue> transformResults(List<NameValue> searchResults, String t) {
        if ("gender".equals(t)) {
            if (searchResults.size() > 0) {
                for (NameValue searchResult : searchResults) {
                    String gender = searchResult.getName();
                    if (gender.equals("F")) {
                        searchResult.setName("女");
                    } else if (gender.equals("M")) {
                        searchResult.setName("男");
                    }
                }
            }
        } else if ("age".equals(t)) {
            Double totalAmount20 = new Double(0);
            Double totalAmount20to29 = new Double(0);
            Double totalAmount30 = new Double(0);
            if (searchResults.size() > 0) {
                for (NameValue searchResult : searchResults) {
                    Integer age = Integer.parseInt(searchResult.getName());
                    Double value = Double.parseDouble(searchResult.getValue().toString());
                    if (age < 20) {
                        totalAmount20 += value;
                    } else if (age <= 29) {
                        totalAmount20to29 += value;
                    } else {
                        totalAmount30 += value;
                    }
                }
                searchResults.clear();
                searchResults.add(new NameValue("20岁以下", totalAmount20));
                searchResults.add(new NameValue("20到29岁", totalAmount20to29));
                searchResults.add(new NameValue("30岁以上", totalAmount30));
            }
        }
        return searchResults;
    }
}
