package de.thi.wechat.controller;

import com.alibaba.fastjson.JSONObject;
import de.thi.wechat.common.BaseResponseVO;
import de.thi.wechat.config.WechatTemplateProperties;
import de.thi.wechat.service.WechatTemplateService;
import de.thi.wechat.utils.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;

@RestController
@RequestMapping("/wechat")
public class WechatTemplateController {
    @Autowired
    private WechatTemplateService wechatTemplateService;

    @Autowired
    private WechatTemplateProperties wechatTemplateProperties;

    @GetMapping("/getTemplate")
    public BaseResponseVO getTemplate() {
        WechatTemplateProperties.WechatTemplate template = wechatTemplateService.getTemplate();
        HashMap<String, Object> resultMap = new HashMap<>();
        resultMap.put("templateId", template.getTemplateId());
        resultMap.put("template", FileUtils.readFile2JsonArray(template.getTemplatePath()));
        return BaseResponseVO.success(resultMap);
    }


    @GetMapping("/getTemplateResult")
    public BaseResponseVO getTemplateResult(@RequestParam(value = "templateId", required = false) String templateId) {
        JSONObject result = wechatTemplateService.getTemplateResult(templateId);
        return BaseResponseVO.success(result);
    }

    @PostMapping("/recoard")
    public BaseResponseVO recoard(@RequestBody JSONObject result) {
        wechatTemplateService.recordTemplateResult(result);
        return BaseResponseVO.success();
    }

}
