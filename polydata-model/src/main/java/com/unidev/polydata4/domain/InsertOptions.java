package com.unidev.polydata4.domain;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Polydata insert options.
 */
@Builder
@Data
@RequiredArgsConstructor
public class InsertOptions {

    @Getter
    private final boolean skipIndex;
    @Getter
    private final BasicPoly additionalOptions;


    public static InsertOptions defaultInsertOptions() {
        return InsertOptions.builder().skipIndex(false).additionalOptions(new BasicPoly()).build();
    }
}
