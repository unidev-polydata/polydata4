package com.unidev.polydata4.api.packer;

public interface BytePacker {

    byte[] pack(byte[] bytes);

    byte[] unpack(byte[] bytes);

}
