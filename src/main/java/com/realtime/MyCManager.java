package com.realtime;

import java.io.InputStream;
import java.util.Properties;

public class MyCManager {

	   //˽�����ö���
    private static Properties prop = new Properties();


    static {
        try {
            //��ȡ�����ļ�������
            String filePath = MyCManager.class.getClassLoader().getResource("configuration.properties").getFile();
            System.out.println("the path1"+filePath);


            //File my = new File(filePath);
            //InputStreamReader isr = new InputStreamReader(new FileInputStream(my), StandardCharsets.UTF_8);
            InputStream in = MyCManager.class
                    .getClassLoader().getResourceAsStream("configuration.properties");
            System.out.println("the path2"+in);

            //�������ö���
            prop.load(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * ��ȡָ��key��Ӧ��value
     *
     * @param key
     * @return value
     */
    public static String getProperty(String key) {
        return prop.getProperty(key);
    }

    /**
     * ��ȡ�������͵�������
     *
     * @param key
     * @return value
     */
    public static Integer getInteger(String key) {
        String value = getProperty(key);
        try {
            return Integer.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * ��ȡ�������͵�������
     *
     * @param key
     * @return value
     */
    public static Boolean getBoolean(String key) {
        String value = getProperty(key);
        try {
            return Boolean.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * ��ȡLong���͵�������
     *
     * @param key
     * @return
     */
    public static Long getLong(String key) {
        String value = getProperty(key);
        try {
            return Long.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0L;
    }
    
    public static void main(String[] args) throws InterruptedException {
    	
    	System.out.println(MyCManager.getProperty(MyConstants.SPARK_MASTER));
    	
    }
    
}


