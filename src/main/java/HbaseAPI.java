import javafx.scene.control.Tab;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseAPI {
    public static void main(String[] args) throws IOException {

//      1. 创建配置 -> 配置设置集群 -> 通过配置建立链接 -> 通过连接获取admin
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","hadoop201,hadoop202,hadoop203");
        Connection connection = ConnectionFactory.createConnection(conf);
//        Admin admin = connection.getAdmin();
        Table table = connection.getTable(TableName.valueOf("test:student"));


//        Put put = new Put(Bytes.toBytes(rowkey));
//        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes("lisi"));
//        table.put(put);

//        Scan scan = new Scan();
//        ResultScanner scanner = table.getScanner(scan);
//        for (Result result : scanner) {
//            for (Cell cell : result.rawCells()) {
//                System.out.println(Bytes.toString(cell.getValueArray()));
//            }
//        }
//        Delete delete = new Delete(Bytes.toBytes(rowKey));
//// 指定列族删除数据
//// delete.addFamily(Bytes.toBytes(cf));
//// 指定列族:列删除数据(所有版本)
//// delete.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));
//// 指定列族:列删除数据(指定版本)
//// delete.addColumns(Bytes.toBytes(cf), Bytes.toBytes(cn));
//
//// 执行删除数据操作
//        table.delete(delete);
//        table.close();

        String rowkey = "1002";
        Get get = new Get(Bytes.toBytes(rowkey));
        get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"));
        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            System.out.println("===========rawData==========");
            System.out.println(Bytes.toString(cell.getFamilyArray()));
            System.out.println(Bytes.toString(cell.getQualifierArray()));
            System.out.println(Bytes.toString(cell.getValueArray()));


            System.out.println("==========CellUtils.cloneMethods===========");
            System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println("==========ToStringMethods==========");
            System.out.println("CF:" + Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getValueLength())+
                    ",CN:" + Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength()) +
                    ",Value:" + Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength()));
        }


//      2. 创建命名空间：
//        NamespaceDescriptor nd = NamespaceDescriptor.create("test").build();
//        admin.createNamespace(nd);
//        System.out.println("nd created");

//      3. 创建表：
//        String tn = "emp";
//        String[] columns = {"info","detail"};
//        TableDescriptorBuilder td = TableDescriptorBuilder.newBuilder(TableName.valueOf(tn));
//        for(String col :columns){
//            ColumnFamilyDescriptorBuilder cfb = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(col));
//            td.setColumnFamily(cfb.build());
//        }
//        admin.createTable(td.build());


//        NamespaceDescriptor nd = NamespaceDescriptor.create("atguigu").build();
//        admin.createNamespace(nd);
//        System.out.println("nd created");
//
//      TableName tableName = TableName.valueOf("test:teacher");
//       boolean b = admin.tableExists(tableName);
//      if(b){
//           System.out.println("table exists");
//        }else {
//           System.out.println("table not exist");
//           String tb = "teacher";
//           String[] cf = {"info","detail"};
//          TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(TableName.valueOf(tb));
//          for (String col : cf) {
//              ColumnFamilyDescriptorBuilder cfdb = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(col));
//              tdb.setColumnFamily(cfdb.build());
//          }
//          admin.createTable(tdb.build());
//      }
//        boolean res = admin.tableExists(TableName.valueOf("teacher"));
//        System.out.println(res);
//        TableName name = TableName.valueOf("teacher");
//        admin.disableTable(name);
//        admin.deleteTable(name);
//        admin.close();
        connection.close();
    }
}
