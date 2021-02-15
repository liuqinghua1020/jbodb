package com.shark.jbodb;

import lombok.Builder;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.shark.jbodb.DB.PAGE_SIZE;

/**
 * 一个page 映射到内存的结构
 */
@Getter
@Builder
public class Node implements Comparable<Node>{


    // 其所在 bucket 的指针
    private Bucket bucket;

    private boolean leaf;

    private boolean unbalanced;

    private boolean spilled;

    private byte[] key;

    private long pgid;

    private Node parent;

    private List<Node> children;

    private List<Entry> entries;

    //在本节点插入Key和Value
    public void put(byte[] oldKey, byte[] newKey, byte[] value, long pgid, int bucketLeafFlag) {
    }

    /**
     * 返回一个 page header + inode 内容的大小的总共字节数
     * @return
     */
    public int size() {
        return 0;
    }

    /**
     *  将 node 序列化到 page中,主要是将 node中的inode信息序列化到page里面
     */

    public void write(Page page) {
    }

    //节点的inode进行分裂
    public void spill() {

        Tx tx = this.getBucket().getTx();

        if(this.isSpilled()){//已经分裂过了
            return;
        }

        //对子节点排序
        Collections.sort(this.children);
        for(Node child:children){
            child.spill();
        }

        this.children.clear();

        // Split nodes into appropriate sizes. The first node will always be n.
        List<Node> nodes = this.split(PAGE_SIZE);

        for(Node node:nodes){

            // Add node's page to the freelist if it's not new.
            if(node.getPgid() > 0){
                tx.getDb().getFreeList().free(tx.getMeta().getTxid(), tx.getPage(node.getPgid()));
            }



            // Allocate contiguous space for the node.
            Page page = tx.allocate(this.size() + 1);

            if(page.getPgid() >= tx.getMeta().getPgid()){
                throw new RuntimeException(String.format("pgid (%d) above high water mark (%d)", page.getPgid(), tx.getMeta().getPgid()));
            }

            node.pgid = page.getPgid();
            node.write(page);
            node.spilled = true;

            // Insert into parent inodes.
            if(node.parent != null){
                byte[] key = node.key;
                if (key == null) {
                    key = node.getEntries().get(0).getKey();
                }

                node.parent.put(key, node.getEntries().get(0).getKey(), null, node.pgid, 0);
                node.key = node.getEntries().get(0).getKey();
            }
        }


        if(this.parent != null && this.parent.pgid == 0) {
            this.children = null;
            this.parent.spill();
        }

    }

    //构造新的节点
    private List<Node> split(int pageSize) {
        List<Node> nodes = new ArrayList<>();

        Node node = this;


        for(;;){
            Node[] res = node.splitTwo(pageSize);
            assert res.length == 2;
            nodes.add(res[0]);
            if(res[1] == null){
                break;
            }
            node = res[1];
        }

        return nodes;
    }

    //node[].length == 2
    private Node[] splitTwo(int pageSize) {
        return null;
    }

    //获取 spill之后新的root节点，主要是MVCC使用
    public Node root() {
        return null;
    }

    @Override
    public int compareTo(Node o) {
        return 0;
    }
}
