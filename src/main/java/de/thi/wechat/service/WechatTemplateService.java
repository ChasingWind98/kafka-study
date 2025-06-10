package de.thi.wechat.service;

import com.alibaba.fastjson.JSONObject;
import de.thi.wechat.config.WechatTemplateProperties;
import org.springframework.stereotype.Service;


public interface WechatTemplateService {
    //  获取模板
    WechatTemplateProperties.WechatTemplate getTemplate();

    //记录问卷结果
    void recordTemplateResult(JSONObject result);

    //获取问卷结果
    JSONObject getTemplateResult(String templateId);
}
