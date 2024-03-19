/*
 * Copyright 2023 Greptime Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.greptime.models;

import io.greptime.common.Into;
import io.greptime.v1.Common;

/**
 * Greptime authentication information
 */
public class AuthInfo implements Into<Common.AuthHeader> {

    private final String username;
    private final String password;

    /**
     * Create AuthInfo from username/password.
     */
    public AuthInfo(String username, String password) {
        this.username = username;
        this.password = password;
    }

    public static AuthInfo noAuthorization() {
        return null;
    }

    @Override
    public Common.AuthHeader into() {
        Common.Basic basic = Common.Basic.newBuilder() //
                .setUsername(this.username) //
                .setPassword(this.password) //
                .build();
        return Common.AuthHeader.newBuilder() //
                .setBasic(basic) //
                .build();
    }
}
