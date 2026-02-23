package oracle

import (
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm/schema"
)

type regularIndexModel struct {
	SearchText string `gorm:"column:SEARCH_TEXT;index:idx_regular_search"`
}

func (regularIndexModel) TableName() string {
	return "regular_index_model"
}

type oracleTextIndexTypeOnlyModel struct {
	SearchText string `gorm:"column:SEARCH_TEXT;type:varchar2(4000);index:idx_participant_search,oracle_indextype:CTXSYS.CONTEXT"`
}

func (oracleTextIndexTypeOnlyModel) TableName() string {
	return "oracle_text_index_type_only_model"
}

type oracleTextIndexWithParamsModel struct {
	SearchText string `gorm:"column:SEARCH_TEXT;type:varchar2(4000);index:idx_participant_search,oracle_indextype:CTXSYS.CONTEXT,oracle_parameters:'SYNC (ON COMMIT)'"`
}

func (oracleTextIndexWithParamsModel) TableName() string {
	return "oracle_text_index_with_params_model"
}

type oracleTextIndexUniqueInvalidModel struct {
	SearchText string `gorm:"column:SEARCH_TEXT;type:varchar2(4000);index:idx_participant_search,unique,oracle_indextype:CTXSYS.CONTEXT"`
}

func (oracleTextIndexUniqueInvalidModel) TableName() string {
	return "oracle_text_index_unique_invalid_model"
}

type oracleTextIndexMalformedModel struct {
	SearchText string `gorm:"column:SEARCH_TEXT;type:varchar2(4000);index:idx_participant_search,oracle_indextype:,oracle_parameters:'SYNC (ON COMMIT)'"`
}

func (oracleTextIndexMalformedModel) TableName() string {
	return "oracle_text_index_malformed_model"
}

type oracleTextIndexUnquotedParamsModel struct {
	SearchText string `gorm:"column:SEARCH_TEXT;type:varchar2(4000);index:idx_participant_search,oracle_indextype:CTXSYS.CONTEXT,oracle_parameters:SYNC (ON COMMIT)"`
}

func (oracleTextIndexUnquotedParamsModel) TableName() string {
	return "oracle_text_index_unquoted_params_model"
}

func TestBuildCreateIndexSQL_RegularIndexUnchanged(t *testing.T) {
	idx := mustLookIndex(t, &regularIndexModel{}, "idx_regular_search")

	sql := buildCreateIndexSQL(idx, oracleDomainIndexConfig{})
	require.Equal(t, "CREATE INDEX ? ON ? ?", sql)
}

func TestBuildCreateIndexSQL_OracleDomainIndexTypeOnly(t *testing.T) {
	idx := mustLookIndex(t, &oracleTextIndexTypeOnlyModel{}, "idx_participant_search")

	cfg, err := parseOracleDomainIndexConfig(idx)
	require.NoError(t, err)
	require.Equal(t, "CTXSYS.CONTEXT", cfg.IndexType)
	require.Equal(t, "", cfg.Parameters)

	sql := buildCreateIndexSQL(idx, cfg)
	require.Equal(t, "CREATE INDEX ? ON ? ? INDEXTYPE IS CTXSYS.CONTEXT", sql)
}

func TestBuildCreateIndexSQL_OracleDomainIndexWithParameters(t *testing.T) {
	idx := mustLookIndex(t, &oracleTextIndexWithParamsModel{}, "idx_participant_search")

	cfg, err := parseOracleDomainIndexConfig(idx)
	require.NoError(t, err)
	require.Equal(t, "CTXSYS.CONTEXT", cfg.IndexType)
	require.Equal(t, "'SYNC (ON COMMIT)'", cfg.Parameters)

	sql := buildCreateIndexSQL(idx, cfg)
	require.Equal(t, "CREATE INDEX ? ON ? ? INDEXTYPE IS CTXSYS.CONTEXT PARAMETERS ('SYNC (ON COMMIT)')", sql)
}

func TestValidateOracleDomainIndexConfig_UniqueIndexReturnsError(t *testing.T) {
	idx := mustLookIndex(t, &oracleTextIndexUniqueInvalidModel{}, "idx_participant_search")

	cfg, err := parseOracleDomainIndexConfig(idx)
	require.NoError(t, err)
	require.Equal(t, "CTXSYS.CONTEXT", cfg.IndexType)

	err = validateOracleDomainIndexConfig(idx, cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot be UNIQUE")
}

func TestParseOracleDomainIndexConfig_MalformedOptionsReturnActionableError(t *testing.T) {
	idx := mustLookIndex(t, &oracleTextIndexMalformedModel{}, "idx_participant_search")

	cfg, err := parseOracleDomainIndexConfig(idx)
	require.NoError(t, err)
	require.Equal(t, "", cfg.IndexType)
	require.Equal(t, "'SYNC (ON COMMIT)'", cfg.Parameters)

	err = validateOracleDomainIndexConfig(idx, cfg)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "missing oracle_indextype"))
}

func TestValidateOracleDomainIndexConfig_UnquotedParametersReturnError(t *testing.T) {
	idx := mustLookIndex(t, &oracleTextIndexUnquotedParamsModel{}, "idx_participant_search")

	cfg, err := parseOracleDomainIndexConfig(idx)
	require.NoError(t, err)
	require.Equal(t, "CTXSYS.CONTEXT", cfg.IndexType)
	require.Equal(t, "SYNC (ON COMMIT)", cfg.Parameters)

	err = validateOracleDomainIndexConfig(idx, cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "must be single-quoted")
}

func mustLookIndex(t *testing.T, model interface{}, name string) *schema.Index {
	t.Helper()

	sch, err := schema.Parse(model, &sync.Map{}, &NamingStrategy{})
	require.NoError(t, err)
	require.NotNil(t, sch)

	idx := sch.LookIndex(name)
	require.NotNil(t, idx)
	return idx
}
