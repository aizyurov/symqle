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

package org.symqle.jdbc;

import org.symqle.common.OutBox;
import org.symqle.common.Parameterizer;
import org.symqle.common.SqlParameters;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * A Parameterizer, which memorizes parameter values in constructor.
 * It provides these values to SqlParameters later. Used by Batcher implementations.
 * @author lvovich
 */
class SavedParameterizer implements Parameterizer {

    final SavedParameters savedParameters;

    /**
     * Constructs from another Parameterizer.
     * @param source
     * @throws SQLException
     */
    SavedParameterizer(final Parameterizer source) throws SQLException {
        savedParameters = new SavedParameters();
        source.setParameters(savedParameters);
    }

    @Override
    public void setParameters(final SqlParameters p) throws SQLException {
        savedParameters.replay(p);
    }

    private class SavedParameters implements SqlParameters {
        private List<SavedOutBox> outBoxes = new ArrayList<SavedOutBox>();

        @Override
        public OutBox next() {
            final SavedOutBox outBox = new SavedOutBox();
            outBoxes.add(outBox);
            return outBox;
        }

        public void replay(SqlParameters parameters) throws SQLException {

            for (SavedOutBox savedOutBox : outBoxes) {
                savedOutBox.replay(parameters.next());
            }

        }
    }

    private class SavedOutBox implements OutBox {

        private final List<ValueSetter> setters = new ArrayList<ValueSetter>();

        @Override
        public void setBoolean(final Boolean x) throws SQLException {
            setters.add(new ValueSetter() {
                @Override
                public void setValue(final OutBox outBox) throws SQLException {
                    outBox.setBoolean(x);
                }
            });
        }

        @Override
        public void setByte(final Byte x) throws SQLException {
            setters.add(new ValueSetter() {
                @Override
                public void setValue(final OutBox outBox) throws SQLException {
                    outBox.setByte(x);
                }
            });
        }

        @Override
        public void setShort(final Short x) throws SQLException {
            setters.add(new ValueSetter() {
                @Override
                public void setValue(final OutBox outBox) throws SQLException {
                    outBox.setShort(x);
                }
            });
        }

        @Override
        public void setInt(final Integer x) throws SQLException {
            setters.add(new ValueSetter() {
                @Override
                public void setValue(final OutBox outBox) throws SQLException {
                    outBox.setInt(x);
                }
            });
        }

        @Override
        public void setLong(final Long x) throws SQLException {
            setters.add(new ValueSetter() {
                @Override
                public void setValue(final OutBox outBox) throws SQLException {
                    outBox.setLong(x);
                }
            });
        }

        @Override
        public void setFloat(final Float x) throws SQLException {
            setters.add(new ValueSetter() {
                @Override
                public void setValue(final OutBox outBox) throws SQLException {
                    outBox.setFloat(x);
                }
            });
        }

        @Override
        public void setDouble(final Double x) throws SQLException {
            setters.add(new ValueSetter() {
                @Override
                public void setValue(final OutBox outBox) throws SQLException {
                    outBox.setDouble(x);
                }
            });
        }

        @Override
        public void setBigDecimal(final BigDecimal x) throws SQLException {
            setters.add(new ValueSetter() {
                @Override
                public void setValue(final OutBox outBox) throws SQLException {
                    outBox.setBigDecimal(x);
                }
            });
        }

        @Override
        public void setString(final String x) throws SQLException {
            setters.add(new ValueSetter() {
                @Override
                public void setValue(final OutBox outBox) throws SQLException {
                    outBox.setString(x);
                }
            });
        }

        @Override
        public void setDate(final Date x) throws SQLException {
            setters.add(new ValueSetter() {
                @Override
                public void setValue(final OutBox outBox) throws SQLException {
                    outBox.setDate(x);
                }
            });
        }

        @Override
        public void setTime(final Time x) throws SQLException {
            setters.add(new ValueSetter() {
                @Override
                public void setValue(final OutBox outBox) throws SQLException {
                    outBox.setTime(x);
                }
            });
        }

        @Override
        public void setTimestamp(final Timestamp x) throws SQLException {
            setters.add(new ValueSetter() {
                @Override
                public void setValue(final OutBox outBox) throws SQLException {
                    outBox.setTimestamp(x);
                }
            });
        }

        @Override
        public void setBytes(final byte[] x) throws SQLException {
            setters.add(new ValueSetter() {
                @Override
                public void setValue(final OutBox outBox) throws SQLException {
                    outBox.setBytes(x);
                }
            });
        }

        public void replay(OutBox outBox) throws SQLException {
            for (ValueSetter setter : setters) {
                setter.setValue(outBox);
            }
        }
    }

    private interface ValueSetter {
        void setValue(final OutBox outBox) throws SQLException;
    }
}
