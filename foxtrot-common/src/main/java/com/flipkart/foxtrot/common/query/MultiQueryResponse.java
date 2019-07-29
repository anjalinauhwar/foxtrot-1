package com.flipkart.foxtrot.common.query;
/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p>
<<<<<<< HEAD
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
=======
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
>>>>>>> phonepe-develop
 */

import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.Opcodes;
import com.flipkart.foxtrot.common.ResponseVisitor;
<<<<<<< HEAD
import java.util.Map;
import lombok.Data;

=======
import lombok.Data;

import java.util.Map;

>>>>>>> phonepe-develop
/***
 Created by nitish.goyal on 22/08/18
 ***/
@Data
public class MultiQueryResponse extends ActionResponse {

    private Map<String, ActionResponse> responses;

    public MultiQueryResponse() {
        super(Opcodes.MULTI_QUERY);
    }

    public MultiQueryResponse(Map<String, ActionResponse> responses) {
        super(Opcodes.MULTI_QUERY);
        this.responses = responses;
    }

    @Override
    public void accept(ResponseVisitor visitor) {
        visitor.visit(this);
    }
}
