package org.murat;

import java.util.List;

public class getLongestCode {
    int execute(String s, List<Integer> L) {
        int a = 0;
        for (Integer i : L) {
            for (int j = 0; j < i.toString().length(); j++) {
                char is = i.toString().charAt(j);
                if (s.charAt(j) == is) {
                    if(j == i.toString().length() -1) {
                        a = i;
                    }
                } else {
                    break;
                }
            }
        }
        return a;
    }
}
