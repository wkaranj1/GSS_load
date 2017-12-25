/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gss_load;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author wilson.karanja
 */
public class file_object {
    public String STUDY_PROTOCOL;
    public String VENDOR;
    public List<String> LIST1;
    public List<String> LIST2;
    
    
    file_object(String sp, String vd, List<String> lst, List<String> lst1){
       STUDY_PROTOCOL  = sp;
        VENDOR = vd;
        LIST1 = lst;
        LIST2 = lst1;
    }

    public void setSTUDY_PROTOCOL(String STUDY_PROTOCOL) {
        this.STUDY_PROTOCOL = STUDY_PROTOCOL;
    }

    public void setVENDOR(String VENDOR) {
        this.VENDOR = VENDOR;
    }

    public void setLIST1(List<String> LIST1) {
        this.LIST1 = LIST1;
    }

    public void setLIST2(List LIST2) {
        this.LIST2 = LIST2;
    }

    public String getSTUDY_PROTOCOL() {
        return STUDY_PROTOCOL;
    }

    public String getVENDOR() {
        return VENDOR;
    }

    public List<String> getLIST1() {
        return LIST1;
    }

    public List<String> getLIST2() {
        return LIST2;
    }
}
