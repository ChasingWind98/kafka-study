package de.thi.wechat.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Data
@Configuration
@ConfigurationProperties(prefix = "wechat.template")
public class WechatTemplateProperties {
    private List<WechatTemplate> templates;
    //0-文件获取
    private int templateType;
    //存储结果文件
    private String templateResultFilePath;

    @Data
    public static class WechatTemplate {
        private String templateId;
        private String templatePath;
        private Boolean isValid;

    }
}
