package com.unidev.polydata4.api.packer;

public class NoOpBytePacker implements BytePacker {

  @Override
  public byte[] pack(byte[] bytes) {
    return bytes;
  }

  @Override
  public byte[] unpack(byte[] bytes) {
    return bytes;
  }
}
