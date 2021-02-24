package com.shark.jbodb;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
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

    public static final int leafMinKeys = 1;
    public static final int branchMinKeys = 2;


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
    public void put(byte[] oldKey, byte[] newKey, byte[] value, long pgid, int flag) {

        assert pgid < this.getBucket().getTx().getMeta().getPgid();
        assert oldKey.length > 0;
        assert newKey.length > 0;


        Entry entry = Entry.builder().key(newKey).flags(flag).value(value).pgid(pgid).build();

        int index = this.findInEntries(oldKey);
        if(index >= this.getEntries().size()){
            this.getEntries().add(entry);
        }

        //replace old one
        this.getEntries().set(index, entry);

    }

    /**
     * 返回一个 page header + inode 内容的大小的总共字节数
     * @return
     */
    public int size() {
        //TODO
        return 0;
    }

    /**
     *  将 node 序列化到 page中,主要是将 node中的inode信息序列化到page里面
     */

    public void write(Page page) {
        //TODO
    }

    /**
     * 删除节点之后导致的不平衡需要处理
     */
    public void rebalance(){

        if(!this.unbalanced){
            return;
        }

        this.unbalanced = false;

        int threshold = PAGE_SIZE / 4;

        /**
         * 这种情况下不需要合并操作，直接返回。
         */
        if(this.size() > threshold && this.getEntries().size() > this.minKeys()){
            return ;
        }


        // 根节点合并需要单独处理
        if(this.getParent() == null){

        }

        if(this.getChildren() == null || this.getChildren().isEmpty()){
            this.getParent().delete(this.getKey());
            this.getParent().removeChildren(this);
            this.getBucket().getNodes().remove(this.pgid);
            this.free();
            this.getParent().rebalance();
        }

        assert this.getParent().getChildren().size() > 1;

        // Destination node is right sibling if idx == 0, otherwise left sibling.
        Node target;
        boolean useNextSibling = (this.getParent().childIndex(this) == 0);
        if(useNextSibling){
            target = this.nextSibling();
        }else{
            target = this.prevSibling();
        }

        // If both this node and the target node are too small then merge them.
        if(useNextSibling){

            /**
             * 删除 子节点所属的Node
             */
            // Reparent all child nodes being moved.
            for(Entry entry:target.getEntries()){
                Node child = this.getBucket().getNodes().get(entry.getPgid());
                if( child != null){
                    child.getParent().removeChildren(child);
                    child.setParent(this);
                    child.getParent().getChildren().add(child);
                }
            }

            // Copy over inodes from target and remove target.
            this.getEntries().addAll(target.getEntries());
            this.getParent().delete(target.key);
            this.getParent().removeChildren(target);

            this.getBucket().getNodes().remove(target.getPgid());

            target.free();


        }else{
            // Reparent all child nodes being moved.
            for(Entry entry:this.getEntries()){
                Node child = this.getBucket().getNodes().get(entry.getPgid());
                if( child != null){
                    child.getParent().removeChildren(child);
                    child.setParent(target);
                    child.getParent().getChildren().add(child);
                }
            }

            // Copy over inodes from target and remove target.
            target.getEntries().addAll(this.getEntries());
            this.getParent().delete(target.key);
            this.getParent().removeChildren(target);
            this.getBucket().getNodes().remove(this.getPgid());
            target.free();
        }

    }

    //寻找前一个兄弟节点（如果节点只有有前后指针是否会好一点？）
    private Node prevSibling() {
        //TODO
        return null;
    }

    //寻找下一个兄弟节点（如果节点只有有前后指针是否会好一点？）
    private Node nextSibling() {
        //TODO
        return null;
    }

    //返回子节点在本节点的位置
    private int childIndex(Node child) {
        //TODO
        return -1;
    }

    //释放本节点，可以将其放到freelist上
    private void free() {

        if(this.getPgid() != 0){ //表示是由存储介质加载上来的页面,可以对其进行释放。
            this.getBucket().getTx().getDb().getFreeList().free(this.getBucket().getTx().getMeta().getTxid(),
                    this.getBucket().getTx().getPage(this.getPgid()));
            this.pgid = 0;
        }

    }

    //从 children中 删除子节点
    private void removeChildren(Node node) {
    }

    //从 entries 中删除 子节点节点的key
    private void delete(byte[] key) {

        int index = this.findInEntries(key);

        if(index < 0 || index >= this.getEntries().size() ||
                !Arrays.equals(this.getEntries().get(index).getKey(), key)){
            return;
        }

        this.removeEntry(index);
        this.setUnbalanced();
    }

    private void removeEntry(int index) {

        assert index >= 0;
        assert index < this.getEntries().size();
        this.getEntries().remove(index);

    }

    private void setUnbalanced() {
        this.unbalanced = true;
    }

    /**
     * 在 node 的 entry 中查找指定的key的 下标
     * 如果没有查找得到，返回 -1；
     * @param key
     * @return
     */
    private int findInEntries(byte[] key) {
        return -1;
    }

    private int minKeys() {
        if(this.isLeaf()){
            return leafMinKeys;
        }

        return branchMinKeys;
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


    //获取 spill之后新的root节点，主要是MVCC使用
    public Node root() {
        return null;
    }

    @Override
    public int compareTo(Node o) {
        return 0;
    }

    // read initializes the node from a page.
    public void read(Page page) {
        //TODO
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
}
