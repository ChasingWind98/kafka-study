package de.thi.wechat.service.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import de.thi.wechat.config.WechatTemplateProperties;
import de.thi.wechat.service.WechatTemplateService;
import de.thi.wechat.utils.FileUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class WechatTemplateServiceImpl implements WechatTemplateService {

    @Autowired
    private WechatTemplateProperties wechatTemplateProperties;

    @Autowired
    private Producer producer;

    @Override
    public WechatTemplateProperties.WechatTemplate getTemplate() {
        return wechatTemplateProperties.getTemplates().stream()
                .filter(WechatTemplateProperties.WechatTemplate::getIsValid)
                .findFirst()
                .get();
    }

    @Override
    public void recordTemplateResult(JSONObject recoard) {
        String topic = "Questionnaire";
        String templateId = recoard.getString("template_id");
        JSONArray recoardInfo = recoard.getJSONArray("result");
        ProducerRecord<String, JSONArray> record = new ProducerRecord<>(topic, templateId, recoardInfo);
        //虽然Kafka配置 ACKS_CONFIG 已经确保  仅有一次发送了  但是在这里如果出现异常 我们可以继续操作
        producer.send(record);

    }


    @Override
    public JSONObject getTemplateResult(String templateId) {
        if (wechatTemplateProperties.getTemplateType() == 0) {
            return FileUtils.readFile2JsonObject(wechatTemplateProperties.getTemplateResultFilePath())
                    .orElse(new JSONObject());
        }
        return null;
    }
}
