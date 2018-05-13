package yangqi.utils;

import org.ho.yaml.Yaml;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

/**
 * loadYaml file
 */
public class PropertiesUtil {
    private final static  String yamlConfig="monitor.yaml";

    public static <T> T loadConfig(String _yamlConfig,Class<T> object){
        String config=yamlConfig;
        if(_yamlConfig!=null && !_yamlConfig.trim().equals("")){
            config=_yamlConfig;
        }
        File file=new File(config);
        try {
            return (T)Yaml.loadType(file,object);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }


    public static void main(String[] args) {
        Map father =loadConfig("/opt/workspace/zookeeper/src/main/resources/monitor.yaml",HashMap.class);
        for(Object key:father.keySet()){
            System.out.println(key+":\t"+father.get(key).toString());
        }
    }

}
