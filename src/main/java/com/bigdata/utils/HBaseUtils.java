package com.bigdata.utils;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseUtils {

    @Autowired
    Connection conn;

    public boolean createTable(String tableName) {
        Admin admin = null;
        TableName table = TableName.valueOf(tableName);
        try {
            admin = conn.getAdmin();
            if (!admin.tableExists(table)) {
                System.out.println(tableName + "table not Exists");
                HTableDescriptor descriptor = new HTableDescriptor(table);
                descriptor.addFamily(new HColumnDescriptor("cf".getBytes()));
                admin.createTable(descriptor);
                return true;
            } else {
                System.out.println(tableName + "table Exists");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 写图像数据
     *
     * @param tableName
     * @param imgPath
     */
    public void addImgData(String tableName, String rowkey, String cf, String column, String imgPath) {
        Table table = null;
        String imgData = null;
        File files = new File(imgPath);
        File[] listFiles = files.listFiles();
        for (File file : listFiles) {
            String imgName = file.getName();
            String imgFormat = imgName.substring(imgName.length() - 3, imgName.length());
            imgData = file.getPath();
        }
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            FileInputStream fis = new FileInputStream(imgData);
            byte[] imgValue = new byte[fis.available()];//读图为流，但字节数组还是空的
            fis.read(imgValue);//将文件内容写入字节数组
            fis.close();
            Put put = new Put(rowkey.getBytes());
            put.add(cf.getBytes(), column.getBytes(), imgValue);
            table.put(put);
        } catch (IOException e) {
            System.out.println(imgPath + "导入失败！");
        }
    }

    /**
     * 批量写图片
     *
     * @param tableName
     * @param rowkey
     * @param cf
     * @param column
     * @param imgValue
     */
    public void batchPutImg(String tableName, String rowkey, String cf, String column, byte[] imgValue) {
        Table table = null;
        try {
            List<Put> putList = new ArrayList<Put>();
            Put put = new Put(rowkey.getBytes());
            put.add(cf.getBytes(), column.getBytes(), imgValue);
            putList.add(put);
            if (putList.size() == 100) {
                table = conn.getTable(TableName.valueOf(tableName));
                table.put(putList);
                putList.clear();
            }
            // table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取图像属性
     *
     * @param imgPath
     */
    public void getImgProperties(String imgPath) {
        File files = new File(imgPath);
        File[] listFiles = files.listFiles();
        FileInputStream fis = null;
        try {
            int i = 0;
            for (File file : listFiles) {
                String imgName = file.getName();
                String imgFormat = imgName.substring(imgName.length() - 3, imgName.length());
                String imgData = file.getPath();
                fis = new FileInputStream(imgData);
                byte[] imgValue = new byte[fis.available()];
                fis.read(imgValue);//将文件内容写入字节数组
                String rowkey = String.valueOf(i++);
                batchPutImg("student1", rowkey, "cf", "img", imgValue);
                System.out.println("共计：" + rowkey + "张图片");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {

                fis.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * 获取图片数据,输出到本地
     */
    public void getImgData(String tableName, String imgOutPath) {
        Table table = null;
        ResultScanner scanner = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            scanner = table.getScanner(scan);
            FileOutputStream fos = null;
            int i = 0;
            for (Result result : scanner) {
                byte[] bs = result.value();//保存get result的结果，字节数组形式
                File file = new File(imgOutPath + (i++) + ".jpg");//将输出的二进制流转化后的图片的路径
                fos = new FileOutputStream(file);
                fos.write(bs);
            }
            table.close();
        } catch (IOException e) {
            System.out.println("读取表失败！");
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
