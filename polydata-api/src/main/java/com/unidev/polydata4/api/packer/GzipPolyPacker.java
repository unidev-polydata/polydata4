package com.unidev.polydata4.api.packer;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonFactoryBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.unidev.polydata4.domain.BasicPoly;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

@Slf4j
public class GzipPolyPacker implements PolyPacker {

  @Getter
  @Setter
  private ObjectMapper objectMapper = objectMapper();

  public byte[] packPoly(BasicPoly poly) throws IOException {
    String value = objectMapper.writeValueAsString(poly);
    return gzipCompress(value.getBytes());
  }

  public BasicPoly unPackPoly(InputStream stream) throws IOException {
    return objectMapper.readValue(gzipUncompress(IOUtils.toByteArray(stream)), BasicPoly.class);
  }

  public static byte[] gzipCompress(byte[] uncompressedData) {
    byte[] result = new byte[]{};
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream(uncompressedData.length);
        GZIPOutputStream gzipOS = new GZIPOutputStream(bos)) {
      gzipOS.write(uncompressedData);
      gzipOS.close();
      result = bos.toByteArray();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return result;
  }

  public static byte[] gzipUncompress(byte[] compressedData) {
    byte[] result = new byte[]{};
    try (ByteArrayInputStream bis = new ByteArrayInputStream(compressedData);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        GZIPInputStream gzipIS = new GZIPInputStream(bis)) {
      byte[] buffer = new byte[1024];
      int len;
      while ((len = gzipIS.read(buffer)) != -1) {
        bos.write(buffer, 0, len);
      }
      result = bos.toByteArray();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return result;
  }

  protected ObjectMapper objectMapper() {
    return objectMapper = new ObjectMapper(
        new JsonFactoryBuilder()
            .configure(JsonFactory.Feature.INTERN_FIELD_NAMES, false)
            .configure(JsonFactory.Feature.CANONICALIZE_FIELD_NAMES, false)
            .build()
    );
  }

}
