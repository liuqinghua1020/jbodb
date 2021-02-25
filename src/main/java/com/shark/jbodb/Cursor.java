package com.shark.jbodb;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.Stack;

import static com.shark.jbodb.PageFlag.BranchPageFlag;
import static com.shark.jbodb.PageFlag.LeafPageFlag;

public class Cursor {


    private Bucket bucket;

    private Stack<ElementRef> stack;


    public Cursor(Bucket bucket){
        this.bucket = bucket;
        this.stack = new Stack<>();
    }

    public Node node() {
        ElementRef elementRef = stack.peek();
        Node node = elementRef.node;
        if(node != null && node.isLeaf()){
            return node;
        }

        //TODO 从 stack 的起始开始，走 B+ 树的层次
        return null;
    }


    // Seek moves the cursor to a given key and returns it.
    // If the key does not exist then the next key is used. If no keys
    // follow, a nil key is returned.
    // The returned key and value are only valid for the life of the transaction.
    public CursorResult seek(byte[] key) {
        CursorResult cursorResult = this.seek0(key);

        ElementRef ref = this.stack.peek();
        // If we ended up after the last element of a page then move to the next one.
        if(ref.index >= ref.count()){
            cursorResult = this.next();
        }

        return cursorResult;
    }

    private CursorResult seek0(byte[] key) {
        //1. 清空当前栈
        this.stack.clear();

        //2.search key
        this.search(key, this.bucket.getRootPgid());

        ElementRef ref = this.stack.peek();

        if(ref.index >= ref.count()){
            return new CursorResult(null, null, 0);
        }


        return this.getKeyValue();
    }

    // keyValue returns the key and value of the current leaf element.
    private CursorResult getKeyValue() {

        ElementRef ref = this.stack.peek();
        if(ref.count() == 0 || ref.index >= ref.count()){
            return new CursorResult(null, null, 0);
        }
        // Retrieve value from node.
        if(ref.node != null){
            Entry entry = ref.node.getEntries().get(ref.index);
            assert entry != null;
            return new CursorResult(entry.getKey(), entry.getValue(), entry.getFlags());
        }


        // Or retrieve value from page.
        LeafPageElement leafPageElement = ref.page.leafPageElement(ref.index);

        return new CursorResult(leafPageElement.getKey(), leafPageElement.getValue(), leafPageElement.getFlag());

    }

    private void search(byte[] key, long pgid) {
        Bucket.PageNodeResult pageNodeResult = this.bucket.pageNode(pgid);
        assert pageNodeResult.getPage() != null;
        assert (pageNodeResult.getPage().getFlag() & LeafPageFlag) != 0;
        assert (pageNodeResult.getPage().getFlag() & BranchPageFlag) != 0;


        ElementRef elementRef = ElementRef.builder().node(pageNodeResult.getNode()).page(pageNodeResult.getPage()).build();
        this.stack.push(elementRef);

        // If we're on a leaf page/node then find the specific node.
        if(elementRef.isLeaf()){
            this.nsearch(key);
            return;
        }

        if(pageNodeResult.getNode() != null){
            this.searchNode(key, pageNodeResult.getNode());
            return;
        }

        this.searchPage(key, pageNodeResult.getPage());


    }

    private void nsearch(byte[] key) {
        ElementRef ref = this.stack.peek();
        Page page = ref.page;
        Node node = ref.node;

        if(node != null){
            FindEntryResult findEntryResult = node.findEntry(key);
            assert findEntryResult.getIndex() < node.getEntries().size();
            ref.index = findEntryResult.getIndex();
        }

        // If we have a page then search its leaf elements.
        FindEntryResult findEntryResult = page.findEntry(key);
        ref.index = findEntryResult.getIndex();
    }


    private void searchNode(byte[] key, Node node) {
        FindEntryResult findEntryResult = node.findEntry(key);
        int index = findEntryResult.getIndex();
        if(!findEntryResult.isExact() && index > 0){
            index --;
        }

        this.stack.peek().index = index;

        // Recursively search to the next page.
        this.search(key, node.getEntries().get(index).getPgid());
    }

    private void searchPage(byte[] key, Page page) {
        BranchPageElement[] branchPageElements = page.branchPageElements();
        //TODO
        FindEntryResult findEntryResult = page.findEntry(key);
        int index = findEntryResult.getIndex();
        if(!findEntryResult.isExact() && index > 0){
            index --;
        }

        this.stack.peek().index = index;
        // Recursively search to the next page.
        this.search(key, branchPageElements[index].getPgid());
    }


    public CursorResult first(){
        //TODO
        return null;
    }

    public CursorResult last(){
        //TODO
        return null;
    }

    public CursorResult next(){
        //TODO
        return null;
    }

    public CursorResult prev(){
        //TODO
        return null;
    }



    @Getter
    public static final class CursorResult{
        byte[] key;
        // TODO 是否有必要换成 wrapperMappedFileBuffer
        byte[] value;
        int flags;

        public CursorResult(){}

        public CursorResult(byte[] key, byte[] value, int flags){
            this.key = key;
            this.value = value;
            this.flags = flags;
        }
    }

    @Builder
    class ElementRef{
        Page page;
        Node node;
        int index;

        public int count() {
            if(this.node != null){
                return this.node.getEntries().size();
            }
            return this.page.getCount();
        }

        /**
         * 判断当前 elem 是否是 leaf 节点
         * @return
         */
        public boolean isLeaf() {
            //TODO
            return false;
        }
    }


    /**
     * 优先从 node 中获取信息，如果node还没加载到内存，则使用对应的page
     */
    class SearchResult{
        Node node;
        Page page;
    }


}
