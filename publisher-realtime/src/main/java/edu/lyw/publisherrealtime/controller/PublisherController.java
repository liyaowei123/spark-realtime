package edu.lyw.publisherrealtime.controller;

import edu.lyw.publisherrealtime.bean.NameValue;
import edu.lyw.publisherrealtime.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
public class PublisherController {

    @Autowired
    PublisherService publisherService;
    /**
     * 日活分析
     * */
    @GetMapping("dauRealtime")
    public Map<String, Object> dauRealtime(@RequestParam("td") String td) {
        return publisherService.doDauRealtime(td);
    }
    /**
     * 交易分析-按照类别（年龄、性别）统计
     * */
    @GetMapping("statsByItem")
    public List<NameValue> statsByItem(@RequestParam("itemName") String itemName,
                                       @RequestParam("date") String date,
                                       @RequestParam("t") String t) {
        return publisherService.doStatsByItem(itemName, date, t);
    }
    /**
     * 交易分析-明细
     * */
    @GetMapping("detailByItem")
    public Map<String, Object> detailByItem(@RequestParam("date") String date,
                                            @RequestParam("itemName") String itemName,
                                            @RequestParam(value = "pageNo", required = false, defaultValue = "1") Integer pageNo,
                                            @RequestParam(value = "pageSize", required = false, defaultValue = "20") Integer pageSize) {
        return publisherService.doDetailByItem(date, itemName, pageNo, pageSize);
    }
}
