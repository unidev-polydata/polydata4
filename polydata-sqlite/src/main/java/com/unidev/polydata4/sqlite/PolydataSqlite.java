package com.unidev.polydata4.sqlite;

import com.unidev.polydata4.api.AbstractPolydata;
import com.unidev.polydata4.domain.BasicPoly;
import com.unidev.polydata4.domain.BasicPolyList;
import com.unidev.polydata4.domain.InsertRequest;
import com.unidev.polydata4.domain.PolyQuery;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;

@RequiredArgsConstructor
@Slf4j
public class PolydataSqlite extends AbstractPolydata {



    @Override
    public void prepareStorage() {

    }

    @Override
    public BasicPoly create(String poly) {
        return null;
    }

    @Override
    public boolean exists(String poly) {
        return false;
    }

    @Override
    public Optional<BasicPoly> config(String poly) {
        return Optional.empty();
    }

    @Override
    public void config(String poly, BasicPoly config) {

    }

    @Override
    public Optional<BasicPoly> metadata(String poly) {
        return Optional.empty();
    }

    @Override
    public void metadata(String poly, BasicPoly metadata) {

    }

    @Override
    public Optional<BasicPoly> index(String poly) {
        return Optional.empty();
    }

    @Override
    public Optional<BasicPoly> indexData(String poly, String indexId) {
        return Optional.empty();
    }

    @Override
    public BasicPolyList insert(String poly, Collection<InsertRequest> insertRequests) {
        return null;
    }

    @Override
    public BasicPolyList update(String poly, Collection<InsertRequest> insertRequests) {
        return null;
    }

    @Override
    public BasicPolyList read(String poly, Set<String> ids) {
        return null;
    }

    @Override
    public BasicPolyList remove(String poly, Set<String> ids) {
        return null;
    }

    @Override
    public BasicPolyList query(String poly, PolyQuery polyQuery) {
        return null;
    }

    @Override
    public Long count(String poly, PolyQuery polyQuery) {
        return null;
    }

    @Override
    public BasicPolyList list() {
        return null;
    }

    @Override
    public void open() {

    }

    @Override
    public void close() throws IOException {

    }
}
