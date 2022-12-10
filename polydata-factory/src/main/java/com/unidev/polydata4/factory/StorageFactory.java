package com.unidev.polydata4.factory;

import com.unidev.polydata4.api.Polydata;
import com.unidev.polydata4.domain.BasicPoly;

import java.util.Optional;

/**
 * Interface for storage factories
 */
public interface StorageFactory {

    /**
     * Return supported storage type.
     */
    String type();

    /**
     * Create polydata from configuration.
     */
    Optional<Polydata> create(BasicPoly config);



}
