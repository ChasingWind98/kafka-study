package de.thi.example.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.*;
import org.checkerframework.checker.units.qual.N;

@Getter
@Setter
@EqualsAndHashCode
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class NetTraffic {
    private String url;
    private String remoteAddr;

    public static void main(String[] args) throws JsonProcessingException {
        NetTraffic netTraffic = new NetTraffic();
        netTraffic.setUrl("www.baidu.com");
        netTraffic.setRemoteAddr("192.168.1.1");
        System.out.println(new ObjectMapper().writeValueAsString(netTraffic));
    }
}
