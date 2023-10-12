package com.example.caiflinkcdc.util;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class StdoutUtils {

    private static DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static String src = null;

    public static void print(String s){
        LocalDateTime now = LocalDateTime.now();

        System.out.println(dtf.format(now)+" "+src.toUpperCase()+" "+s);
    }

    public static void init(String s){
        src = s;
    }

}
