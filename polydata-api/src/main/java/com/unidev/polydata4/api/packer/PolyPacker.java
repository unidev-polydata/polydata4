package com.unidev.polydata4.api.packer;


import com.unidev.polydata4.domain.BasicPoly;

import java.io.InputStream;

/**
 * Poly packer interface
 */
public interface PolyPacker {

  byte[] packPoly(BasicPoly poly) throws Exception;

  BasicPoly unPackPoly(InputStream stream) throws Exception;

}
