package com.shark.jbodb;

import lombok.Getter;

import java.util.Stack;

public class Cursor {


    public Node node() {
        ElementRef elementRef = stack.peek();
        Node node = elementRef.node;
        if(node != null && node.isLeaf()){
            return node;
        }

        //TODO 从 stack 的起始开始，走 B+ 树的层次
        return null;
    }

    class ElementRef{
        Page page;
        Node node;
        int index;
    }

    private Bucket bucket;

    private Stack<ElementRef> stack;


    public Cursor(Bucket bucket){
        this.bucket = bucket;
        this.stack = new Stack<>();
    }

    public CursorResult seek(byte[] key) {
        return null;
    }



    @Getter
    public static final class CursorResult{
        byte[] key;
        // TODO 是否有必要换成 wrapperMappedFileBuffer
        byte[] value;
        int flags;
    }


}
