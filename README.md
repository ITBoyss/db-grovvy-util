# db-grovvy-util
mysql数据库结构转groovy脚本
注意：目前仅支持mysql数据库！
注意：目前仅支持mysql数据库！
注意：目前仅支持mysql数据库！

作用：生成groovy数据库表脚本https://www.jianshu.com/p/dcab2df0a105（groovy使用示例）

main方法程序入口，注意配置生成脚本存放路径
生成的脚本放在src/main/resources/script/db目录
maven项目  jdk1.8中级版
打包命令：项目路径下mvn package
生成的jar包在target目录下
执行jar的命令：java -jar gsg.jar -target脚本存放路径 -url数据库连接路径 -username数据库连接名称 -password数据库连接密码 -driver数据库驱动
