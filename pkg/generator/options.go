/*
 * Warp (C) 2019-2020 MinIO, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package generator

import (
	"errors"
	"math/rand"
)

// Options provides options.
// Use WithXXX functions to set them.
type Options struct {
	src          func(o Options) (Source, error)
	totalSize    int64
	randSize     bool
	csv          CsvOpts
	random       RandomOpts
	randomPrefix int
}

// OptionApplier allows to abstract generator options.
type OptionApplier interface {
	Apply() Option
}

// getSize will return a size for an object.
func (o Options) getSize(rng *rand.Rand) int64 {
	if !o.randSize {
		return o.totalSize
	}
	return GetExpRandSize(rng, o.totalSize)
}

func defaultOptions() Options {
	o := Options{
		src:          newRandom,
		totalSize:    1 << 20,
		csv:          csvOptsDefaults(),
		random:       randomOptsDefaults(),
		randomPrefix: 0,
	}
	return o
}

// WithSize sets the size of the generated data.
func WithSize(n int64) Option {
	return func(o *Options) error {
		if n <= 0 {
			return errors.New("WithSize: 大小必须 > 0")
		}
		if o.randSize && o.totalSize < 256 {
			return errors.New("WithSize: 随机对象的大小至少需要 256 个字节")
		}

		o.totalSize = n
		return nil
	}
}

// WithRandomSize will randomize the size from 1 byte to the total size set.
func WithRandomSize(b bool) Option {
	return func(o *Options) error {
		if o.totalSize > 0 && o.totalSize < 256 {
			return errors.New("WithRandomSize: 随机对象的大小至少需要 256 个字节")
		}
		o.randSize = b
		return nil
	}
}

// WithPrefixSize sets prefix size.
func WithPrefixSize(n int) Option {
	return func(o *Options) error {
		if n < 0 {
			return errors.New("WithPrefixSize: 大小必须 >= 0 和 <= 16")
		}
		if n > 16 {
			return errors.New("WithPrefixSize: 大小必须 >= 0 和 <= 16")
		}
		o.randomPrefix = n
		return nil
	}
}
