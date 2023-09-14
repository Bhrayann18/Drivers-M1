/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package net.cltech.middleware.drivers.app;

import java.io.File;
import java.io.FileFilter;

/**
 *
 * @author fmunoz
 */
public class Filter implements FileFilter
{

    @Override
    public boolean accept(File dir)
    {
        return dir.getAbsolutePath().toLowerCase().endsWith(".xls");
    }
    
}
