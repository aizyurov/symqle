/*
   Copyright 2010-2013 Alexander Izyurov

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.package org.symqle.common;
*/

package org.symqle.querybuilder;

import org.symqle.common.Sql;
import org.symqle.common.SqlParameters;

/**
 * Text-only Sql, no parameters,
 * The  text is provided in the constructor.
 */
public class CustomSql implements Sql {
    final String text;

    /**
     * Constructs with a given text
     * @param text
     */
    public CustomSql(String text) {
        this.text = text;
    }

    @Override
    public String getSqlText() {
        return text;
    }

    @Override
    public void setParameters(SqlParameters p) {
        // do nothing
    }
}
