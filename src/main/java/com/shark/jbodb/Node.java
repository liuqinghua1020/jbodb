package com.shark.jbodb;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.shark.jbodb.BranchPageElement.branchPageElementSize;
import static com.shark.jbodb.DB.*;
import static com.shark.jbodb.LeafPageElement.leafPageElementSize;
import static com.shark.jbodb.Page.PAGEHEADERSIZE;

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

    @Setter
    private Node parent;

    private List<Node> children;

    @Setter
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
        if(this.entries.size() <= (minKeysPerPage*2) || this.sizeLessThan(pageSize) ){
            return new Node[]{this, null};
        }

        BigDecimal fillpercent = new BigDecimal(this.getBucket().getFillPercent());
        if(fillpercent.compareTo(minFillPercent) < 0){
            fillpercent = minFillPercent;
        }else if(fillpercent.compareTo(maxFillPercent) >0 ){
            fillpercent = maxFillPercent;
        }


        int threshold = fillpercent.multiply(new BigDecimal(PAGE_SIZE)).intValue();

        // Determine split position and sizes of the two pages.
        int splitIndex = this.splitIndex(threshold);

        // Split node into two separate nodes.
        // If there's no parent then we'll need to create one.
        if(this.getParent() == null){
            Node parant = Node.builder().children(new ArrayList<>()).build();
            this.setParent(parant);
            parent.addChild(this);
        }

        // Create a new node and add it to the parent.
        Node next = Node.builder().bucket(this.getBucket()).leaf(this.isLeaf())
                .parent(parent).build();
        parent.addChild(next);

        List<Entry> entries = this.getEntries();
        List<Entry> thisNewList = entries.subList(0, splitIndex);
        List<Entry> nextNewList = entries.subList(splitIndex+1, entries.size());
        this.setEntries(thisNewList);
        next.setEntries(nextNewList);
        //clear for gc
        entries.clear();
        entries = null;

        return new Node[]{this, next};
    }

    private void addChild(Node node) {
        this.children.add(node);
    }

    // splitIndex finds the position where a page will fill a given threshold.
    // It returns the index as well as the size of the first page.
    // This is only be called from split().
    private int splitIndex(int threshold) {
        int sz = PAGEHEADERSIZE;

        // Loop until we only have the minimum number of keys required for the second page.
        int i=0;
        int splitIndex = i;
        for(i = 0; i < this.getEntries().size()-minKeysPerPage; i++ ){
            splitIndex = i;
            Entry inode = this.getEntries().get(i);
            int elsize = this.pageElementSize() + inode.getKey().length + inode.getValue().length;

            // If we have at least the minimum number of keys and adding another
            // node would put us over the threshold then exit and return.
            if (i >= minKeysPerPage && sz+elsize > threshold){
                break;
            }

            // Add the element size to the total size.
            sz += elsize;
        }

        return splitIndex;

    }

    private int pageElementSize() {
        if (this.isLeaf()) {
            return leafPageElementSize;
        }
        return branchPageElementSize;
    }

    private boolean sizeLessThan(int pageSize) {
        int sz = PAGEHEADERSIZE;
        int elsz = this.pageElementSize();
        for (int i = 0; i < this.getEntries().size(); i++) {
            Entry item = this.getEntries().get(i);
            sz += elsz + item.getKey().length + item.getValue().length;
            if(sz >= pageSize) {
                return false;
            }
        }
        return true;
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
