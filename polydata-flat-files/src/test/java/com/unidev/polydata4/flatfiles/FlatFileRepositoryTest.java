package com.unidev.polydata4.flatfiles;

import com.unidev.polydata4.domain.BasicPoly;
import com.unidev.polydata4.domain.BasicPolyList;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class FlatFileRepositoryTest {

    @Test
    public void repositoryAdd() {
        FlatFileRepository repository = createRepository();

        BasicPolyList polyList = repository.fetchById(Set.of("test1", "test2"));
        assertNotNull(polyList);
        assertEquals(2, polyList.list().size());

        assertTrue(polyList.hasPoly("test1"));
        assertTrue(polyList.hasPoly("test2"));
    }

    @Test
    public void queryByTag() {
        FlatFileRepository repository = createRepository();
        BasicPolyList list = repository.fetchIndexById("_date", List.of(1));
        assertNotNull(list);
        assertEquals(1, list.list().size());
        assertTrue(list.hasPoly("test1"));
    }

    @Test
    public void fetchingPolysFromIndex() {
        FlatFileRepository repository = createRepository();
        BasicPolyList list = repository.fetchPolysFromIndex("_date");
        assertNotNull(list);
        assertEquals(2, list.list().size());
        assertTrue(list.hasPoly("test1"));
        assertTrue(list.hasPoly("test2"));
    }

    @Test
    void serializationTest() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();

        FlatFileRepository repository = createRepository();

        File file = File.createTempFile("test", ".json");
        file.deleteOnExit();
        objectMapper.writeValue(file, repository);

        FlatFileRepository loadedRepository = objectMapper.readValue(file, FlatFileRepository.class);

        loadedRepository.getPolyById().forEach((id, poly) -> {
            assertTrue(repository.getPolyById().containsKey(id));
            BasicPoly existingPoly = repository.getPolyById().get(id);
            assertEquals(existingPoly.data(), poly.data());
            assertEquals(existingPoly.metadata(), poly.metadata());
        });

        loadedRepository.getPolyIndex().forEach((index, list) -> {
            List<String> existingList = repository.getPolyIndex().get(index);
            assertEquals(existingList, list);
        });

    }


    private FlatFileRepository createRepository() {
        FlatFileRepository repository = new FlatFileRepository();
        repository.add(BasicPoly.newPoly("test1").with(FlatFileRepository.TIMESTAMP_KEY, 1), List.of("_date"));
        repository.add(BasicPoly.newPoly("test2").with(FlatFileRepository.TIMESTAMP_KEY, 2), List.of("_date"));
        return repository;
    }



}
