//go:build go1.18

package protocodec_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/vimeo/galaxycache"
	"github.com/vimeo/galaxycache/protocodec"
	"github.com/vimeo/galaxycache/protocodec/internal/testpbv2"
)

// Test of common-good-case
func TestBackendGetterV2Good(t *testing.T) {
	beGood := func(ctx context.Context, key string) (*testpbv2.TestMessage, error) {
		return &testpbv2.TestMessage{
			Name: proto.String("TestName"),
			City: proto.String("TestCity"),
		}, nil
	}

	be := protocodec.BackendGetterV2(beGood)

	ctx := context.Background()

	// test with a proto codec passed (local common-case)
	{
		pc := protocodec.NewV2[testpbv2.TestMessage]()

		if getErr := be.Get(ctx, "foobar", &pc); getErr != nil {
			t.Errorf("noop Get call failed: %s", getErr)
		}

		pv := pc.Get()
		if pv.City == nil || *pv.City != "TestCity" {
			t.Errorf("unexpected value for City: %v", pv.City)
		}
		if pv.Name == nil || *pv.Name != "TestName" {
			t.Errorf("unexpected value for Name: %v", pv.Name)
		}
	}
	// test with a ByteCodec to exercise the common-case when a remote-fetch is done
	{
		c := galaxycache.ByteCodec{}

		if getErr := be.Get(ctx, "foobar", &c); getErr != nil {
			t.Errorf("noop Get call failed: %s", getErr)
		}

		if len(c) < len("TestName")+len("TestCity") {
			t.Errorf("marshaled bytes too short (less than sum of two string fields)")
		}

		pc := protocodec.NewV2[testpbv2.TestMessage]()

		if umErr := pc.UnmarshalBinary([]byte(c)); umErr != nil {
			t.Errorf("failed to unmarshal bytes: %s", umErr)
		}

		pv := pc.Get()
		if pv.City == nil || *pv.City != "TestCity" {
			t.Errorf("unexpected value for City: %v", pv.City)
		}
		if pv.Name == nil || *pv.Name != "TestName" {
			t.Errorf("unexpected value for Name: %v", pv.Name)
		}
	}
}

func TestBackendGetterV2Bad(t *testing.T) {
	sentinel := errors.New("sentinel error")

	beErrorer := func(ctx context.Context, key string) (*testpbv2.TestMessage, error) {
		return nil, fmt.Errorf("error: %w", sentinel)
	}

	be := protocodec.BackendGetterV2(beErrorer)

	ctx := context.Background()

	// test with a proto codec passed (local common-case)
	{
		pc := protocodec.NewV2[testpbv2.TestMessage]()

		if getErr := be.Get(ctx, "foobar", &pc); getErr == nil {
			t.Errorf("noop Get call didn't fail")
		} else if !errors.Is(getErr, sentinel) {
			t.Errorf("Error from Get did not wrap/equal sentinel")
		}
	}
	// test with a ByteCodec to exercise the common-case when a remote-fetch is done
	{
		c := galaxycache.ByteCodec{}

		if getErr := be.Get(ctx, "foobar", &c); getErr == nil {
			t.Errorf("noop Get call didn't fail")
		} else if !errors.Is(getErr, sentinel) {
			t.Errorf("Error from Get did not wrap/equal sentinel")
		}
	}
}
