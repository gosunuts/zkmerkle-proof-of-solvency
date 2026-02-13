package witness

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/binance/zkmerkle-proof-of-solvency/src/utils"
)

const (
	StatusPublished = iota
	StatusReceived
	StatusFinished
)

const (
	TableNamePrefix = `witness`
)

type (
	WitnessModel interface {
		CreateBatchWitnessTable() error
		DropBatchWitnessTable() error
		GetLatestBatchWitnessHeight() (height int64, err error)
		GetBatchWitnessByHeight(height int64) (witness *BatchWitness, err error)
		UpdateBatchWitnessStatus(witness *BatchWitness, status int64) error
		GetLatestBatchWitness() (witness *BatchWitness, err error)
		GetLatestBatchWitnessByStatus(status int64) (witness *BatchWitness, err error)
		GetAllBatchHeightsByStatus(status int64, limit int, offset int) (witnessHeights []int64, err error)
		GetAndUpdateBatchesWitnessByStatus(beforeStatus, afterStatus int64, count int32) (witness [](*BatchWitness), err error)
		GetAndUpdateBatchesWitnessByHeight(height int, beforeStatus, afterStatus int64) (witness [](*BatchWitness), err error)
		CreateBatchWitness(witness []BatchWitness) error
		GetRowCounts() (count []int64, err error)
	}

	defaultWitnessModel struct {
		table string
		db    *utils.DB
	}

	BatchWitness struct {
		ID          uint64
		CreatedAt   time.Time
		UpdatedAt   time.Time
		DeletedAt   *time.Time
		Height      int64
		WitnessData string
		Status      int64
	}
)

func NewWitnessModel(db *utils.DB, suffix string) WitnessModel {
	return &defaultWitnessModel{
		table: TableNamePrefix + suffix,
		db:    db,
	}
}

func (m *defaultWitnessModel) TableName() string {
	return m.table
}

func (m *defaultWitnessModel) CreateBatchWitnessTable() error {
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		deleted_at TIMESTAMP NULL DEFAULT NULL,
		height BIGINT NOT NULL UNIQUE,
		witness_data LONGTEXT NOT NULL,
		status BIGINT NOT NULL,
		INDEX idx_status (status)
	)`, m.table)
	_, err := m.db.Exec(query)
	return err
}

func (m *defaultWitnessModel) DropBatchWitnessTable() error {
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", m.table)
	_, err := m.db.Exec(query)
	return err
}

func (m *defaultWitnessModel) GetLatestBatchWitnessHeight() (batchNumber int64, err error) {
	var height int64
	query := fmt.Sprintf("SELECT height FROM %s WHERE deleted_at IS NULL ORDER BY height DESC LIMIT 1", m.table)
	row := m.db.QueryRowWithTimeout(query)
	err = row.Scan(&height)
	if err == sql.ErrNoRows {
		return 0, utils.DbErrNotFound
	}
	if err != nil {
		return 0, utils.ConvertMysqlErrToDbErr(err)
	}
	return height, nil
}

func (m *defaultWitnessModel) GetLatestBatchWitness() (witness *BatchWitness, err error) {
	var height int64
	query := fmt.Sprintf("SELECT height FROM %s WHERE deleted_at IS NULL ORDER BY height DESC LIMIT 1", m.table)
	row := m.db.QueryRowWithTimeout(query)
	err = row.Scan(&height)
	if err == sql.ErrNoRows {
		return nil, utils.DbErrNotFound
	}
	if err != nil {
		return nil, utils.ConvertMysqlErrToDbErr(err)
	}

	return m.GetBatchWitnessByHeight(height)
}

func (m *defaultWitnessModel) GetLatestBatchWitnessByStatus(status int64) (witness *BatchWitness, err error) {
	witness = &BatchWitness{}
	query := fmt.Sprintf("SELECT id, created_at, updated_at, deleted_at, height, witness_data, status FROM %s WHERE status = ? AND deleted_at IS NULL LIMIT 1", m.table)
	row := m.db.QueryRowWithTimeout(query, status)
	err = row.Scan(&witness.ID, &witness.CreatedAt, &witness.UpdatedAt, &witness.DeletedAt, &witness.Height, &witness.WitnessData, &witness.Status)
	if err == sql.ErrNoRows {
		return nil, utils.DbErrNotFound
	}
	if err != nil {
		return nil, utils.ConvertMysqlErrToDbErr(err)
	}
	return witness, nil
}

func (m *defaultWitnessModel) GetAndUpdateBatchesWitnessByStatus(beforeStatus, afterStatus int64, count int32) (witnesses [](*BatchWitness), err error) {
	tx, err := m.db.BeginTransaction()
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()

	// Select witnesses with FOR UPDATE lock
	query := fmt.Sprintf("SELECT id, created_at, updated_at, deleted_at, height, witness_data, status FROM %s WHERE status = ? AND deleted_at IS NULL ORDER BY height ASC LIMIT ? FOR UPDATE", m.table)
	rows, err := tx.Query(query, beforeStatus, count)
	if err != nil {
		return nil, utils.ConvertMysqlErrToDbErr(err)
	}
	defer rows.Close()

	for rows.Next() {
		witness := &BatchWitness{}
		err = rows.Scan(&witness.ID, &witness.CreatedAt, &witness.UpdatedAt, &witness.DeletedAt, &witness.Height, &witness.WitnessData, &witness.Status)
		if err != nil {
			return nil, err
		}
		witnesses = append(witnesses, witness)
	}

	if len(witnesses) == 0 {
		return nil, utils.DbErrNotFound
	}

	// Update status for each witness
	updateQuery := fmt.Sprintf("UPDATE %s SET status = ?, updated_at = NOW() WHERE height = ?", m.table)
	for _, w := range witnesses {
		_, err = tx.Exec(updateQuery, afterStatus, w.Height)
		if err != nil {
			return nil, err
		}
	}

	return witnesses, nil
}

func (m *defaultWitnessModel) GetAndUpdateBatchesWitnessByHeight(height int, beforeStatus, afterStatus int64) (witnesses [](*BatchWitness), err error) {
	tx, err := m.db.BeginTransaction()
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()

	// Select witnesses with FOR UPDATE lock
	query := fmt.Sprintf("SELECT id, created_at, updated_at, deleted_at, height, witness_data, status FROM %s WHERE height = ? AND status = ? AND deleted_at IS NULL ORDER BY height ASC", m.table)
	rows, err := tx.Query(query, height, beforeStatus)
	if err != nil {
		return nil, utils.ConvertMysqlErrToDbErr(err)
	}
	defer rows.Close()

	for rows.Next() {
		witness := &BatchWitness{}
		err = rows.Scan(&witness.ID, &witness.CreatedAt, &witness.UpdatedAt, &witness.DeletedAt, &witness.Height, &witness.WitnessData, &witness.Status)
		if err != nil {
			return nil, err
		}
		witnesses = append(witnesses, witness)
	}

	if len(witnesses) == 0 {
		return nil, utils.DbErrNotFound
	}

	// Update status for each witness
	updateQuery := fmt.Sprintf("UPDATE %s SET status = ?, updated_at = NOW() WHERE height = ?", m.table)
	for _, w := range witnesses {
		_, err = tx.Exec(updateQuery, afterStatus, w.Height)
		if err != nil {
			return nil, err
		}
	}

	return witnesses, nil
}

func (m *defaultWitnessModel) GetBatchWitnessByHeight(height int64) (witness *BatchWitness, err error) {
	witness = &BatchWitness{}
	query := fmt.Sprintf("SELECT id, created_at, updated_at, deleted_at, height, witness_data, status FROM %s WHERE height = ? AND deleted_at IS NULL LIMIT 1", m.table)
	row := m.db.QueryRowWithTimeout(query, height)
	err = row.Scan(&witness.ID, &witness.CreatedAt, &witness.UpdatedAt, &witness.DeletedAt, &witness.Height, &witness.WitnessData, &witness.Status)
	if err == sql.ErrNoRows {
		return nil, utils.DbErrNotFound
	}
	if err != nil {
		return nil, utils.ConvertMysqlErrToDbErr(err)
	}
	return witness, nil
}

func (m *defaultWitnessModel) CreateBatchWitness(witness []BatchWitness) error {
	if len(witness) == 0 {
		return nil
	}

	query := fmt.Sprintf("INSERT INTO %s (height, witness_data, status, created_at, updated_at) VALUES (?, ?, ?, NOW(), NOW())", m.table)
	for _, w := range witness {
		_, err := m.db.Exec(query, w.Height, w.WitnessData, w.Status)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *defaultWitnessModel) GetAllBatchHeightsByStatus(status int64, limit int, offset int) (witnessHeights []int64, err error) {
	query := fmt.Sprintf("SELECT height FROM %s WHERE status = ? AND deleted_at IS NULL ORDER BY height ASC LIMIT ? OFFSET ?", m.table)
	rows, err := m.db.QueryWithTimeout(query, status, limit, offset)
	if err != nil {
		return nil, utils.ConvertMysqlErrToDbErr(err)
	}
	defer rows.Close()

	for rows.Next() {
		var height int64
		err = rows.Scan(&height)
		if err != nil {
			return nil, err
		}
		witnessHeights = append(witnessHeights, height)
	}

	if len(witnessHeights) == 0 {
		return nil, utils.DbErrNotFound
	}
	return witnessHeights, nil
}

func (m *defaultWitnessModel) UpdateBatchWitnessStatus(witness *BatchWitness, status int64) error {
	query := fmt.Sprintf("UPDATE %s SET status = ?, updated_at = NOW() WHERE height = ?", m.table)
	_, err := m.db.Exec(query, status, witness.Height)
	return err
}

func (m *defaultWitnessModel) GetRowCounts() (counts []int64, err error) {
	var count int64
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE deleted_at IS NULL", m.table)
	row := m.db.QueryRowWithTimeout(query)
	err = row.Scan(&count)
	if err != nil {
		return nil, utils.ConvertMysqlErrToDbErr(err)
	}
	counts = append(counts, count)

	var publishedCount int64
	query = fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE status = ? AND deleted_at IS NULL", m.table)
	row = m.db.QueryRowWithTimeout(query, StatusPublished)
	err = row.Scan(&publishedCount)
	if err != nil {
		return nil, utils.ConvertMysqlErrToDbErr(err)
	}
	counts = append(counts, publishedCount)

	var pendingCount int64
	row = m.db.QueryRowWithTimeout(query, StatusReceived)
	err = row.Scan(&pendingCount)
	if err != nil {
		return nil, utils.ConvertMysqlErrToDbErr(err)
	}
	counts = append(counts, pendingCount)

	var finishedCount int64
	row = m.db.QueryRowWithTimeout(query, StatusFinished)
	err = row.Scan(&finishedCount)
	if err != nil {
		return nil, utils.ConvertMysqlErrToDbErr(err)
	}
	counts = append(counts, finishedCount)

	return counts, nil
}
