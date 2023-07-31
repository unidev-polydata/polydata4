package com.unidev.polydata4.domain;

import lombok.Builder;

/**
 * Polydata insert options.
 */
@Builder
public record InsertOptions(boolean skipIndex, BasicPoly additionalOptions) {

    public static InsertOptions defaultInsertOptions() {
        return InsertOptions.builder().skipIndex(false).additionalOptions(new BasicPoly()).build();
    }
}
