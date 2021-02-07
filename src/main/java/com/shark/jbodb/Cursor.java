package com.shark.jbodb;

import java.util.Stack;

public class Cursor {

    private Bucket bucket;

    private Stack<ElementRef> stack;


    class ElementRef{
        Page page;
        Node node;
        int index;
    }
}
