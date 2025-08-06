package main

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/turbopuffer/tpuf-benchmark/pkg/template"
	"github.com/xitongsys/parquet-go-source/buffer"
	"github.com/xitongsys/parquet-go/reader"
)

// CohereWikipediaEmbeddings provides template data from Cohere's Wikipedia embeddings.
// See: https://huggingface.co/datasets/Cohere/wikipedia-22-12-en-embeddings
type CohereWikipediaEmbeddings struct {
	logger    *slog.Logger
	lock      sync.Mutex              // protects downloads
	downloads map[string][]chan error // current in progress downloads
}

var _ template.Datasource = (*CohereWikipediaEmbeddings)(nil)

// NewCohereWikipediaEmbeddings creates a new instance of CohereWikipediaEmbeddings.
func NewCohereWikipediaEmbeddings(logger *slog.Logger) *CohereWikipediaEmbeddings {
	return &CohereWikipediaEmbeddings{
		logger:    logger,
		downloads: make(map[string][]chan error),
	}
}

func (c *CohereWikipediaEmbeddings) NewIDSource() template.IDSource {
	return &template.MonotonicIDSource{}
}

func (c *CohereWikipediaEmbeddings) NewVectorSource() template.VectorSource {
	return &cohereVectorSource{
		orig:     c,
		nextFile: 0,
		vectors:  nil,
	}
}

func (c *CohereWikipediaEmbeddings) NewTextSource() template.TextSource {
	return &cohereTextSource{
		rng:      rand.New(rand.NewPCG(42, 69)),
		orig:     c,
		nextFile: 0,
		texts:    nil,
	}
}

func (c *CohereWikipediaEmbeddings) filePath(fileName string) string {
	return filepath.Join("/tmp", fileName)
}

func (c *CohereWikipediaEmbeddings) downloadFile(ctx context.Context, fileName string) error {
	var wait chan error
	c.lock.Lock()
	if _, exists := c.downloads[fileName]; !exists {
		c.downloads[fileName] = []chan error{} // We're responsible for downloading
	} else {
		wait = make(chan error) // We're waiting for an existing download
		c.downloads[fileName] = append(c.downloads[fileName], wait)
	}
	c.lock.Unlock()

	if wait != nil {
		return <-wait
	}

	download := func() error {
		start := time.Now()
		c.logger.Info("downloading file", slog.String("file", fileName))

		url := fmt.Sprintf(
			"https://huggingface.co/datasets/Cohere/wikipedia-22-12-en-embeddings/resolve/main/data/%s?download=true",
			fileName,
		)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return fmt.Errorf("failed to create request for %s: %w", url, err)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return fmt.Errorf("failed to download %s: %w", url, err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("failed to download %s: status code %d", url, resp.StatusCode)
		}

		fp := c.filePath(fileName)
		f, err := os.Create(fp)
		if err != nil {
			return fmt.Errorf("failed to create file %s: %w", fp, err)
		}

		if _, err := f.ReadFrom(resp.Body); err != nil {
			return fmt.Errorf("failed to write to file %s: %w", fp, err)
		}

		if err := f.Close(); err != nil {
			return fmt.Errorf("failed to close file %s: %w", fp, err)
		}

		c.logger.Info(
			"download complete",
			slog.String("file", fileName),
			slog.Duration("took", time.Since(start)),
		)

		return nil
	}

	err := download()

	c.lock.Lock()
	chans := c.downloads[fileName]
	delete(c.downloads, fileName) // Download complete, remove from map
	for _, ch := range chans {
		select {
		case ch <- err:
		default:
		}
	}
	c.lock.Unlock()

	return err
}

type cohereVectorSource struct {
	orig     *CohereWikipediaEmbeddings
	nextFile int
	vectors  []func() ([]float32, bool)
}

func (cvs *cohereVectorSource) Vector(dims int) ([]float32, error) {
	for {
		if len(cvs.vectors) == 0 {
			if err := cvs.loadNextFile(context.Background()); err != nil {
				return nil, fmt.Errorf("failed to load next file: %w", err)
			}
			continue
		}
		vector, ok := cvs.vectors[0]()
		if !ok {
			cvs.vectors = cvs.vectors[1:]
			continue
		}
		return template.TruncateOrExpandVector(vector, dims), nil
	}
}

func (cvs *cohereVectorSource) loadNextFile(ctx context.Context) error {
	fileIdx := cvs.nextFile
	if fileIdx >= len(cohereWikipediaEmbeddingFiles) {
		return errors.New("no more files")
	}
	cvs.nextFile++

	var (
		fname = cohereWikipediaEmbeddingFiles[fileIdx]
		fp    = cvs.orig.filePath(fname)
	)
	if _, err := os.Stat(fp); os.IsNotExist(err) {
		if err := cvs.orig.downloadFile(ctx, fname); err != nil {
			return fmt.Errorf("failed to download file %s: %w", fname, err)
		}
	}

	mmapped, err := template.MemoryMapFile(fp)
	if err != nil {
		return fmt.Errorf("failed to memory map file %s: %w", fp, err)
	}

	vectorSeq, err := readVectorColumn(mmapped.Data, 8, 768)
	if err != nil {
		return fmt.Errorf("failed to read vectors from file %s: %w", fp, err)
	}
	pull, _ := iter.Pull(vectorSeq)
	cvs.vectors = append(cvs.vectors, pull)

	return nil
}

func readVectorColumn(fileContent []byte, column, dims int64) (iter.Seq[[]float32], error) {
	bf := buffer.NewBufferFileFromBytesNoAlloc(fileContent)
	pr, err := reader.NewParquetColumnReader(bf, 1)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet column reader: %w", err)
	}
	n := pr.GetNumRows()
	vectors, _, _, err := pr.ReadColumnByIndex(column, n*dims)
	if err != nil {
		return nil, fmt.Errorf("failed to read column %d: %w", column, err)
	}
	return func(yield func([]float32) bool) {
		for i := range n {
			vector := make([]float32, dims)
			for j := range dims {
				vector[j] = vectors[i*dims+j].(float32)
			}
			if !yield(vector) {
				break
			}
		}
	}, nil
}

type cohereTextSource struct {
	orig     *CohereWikipediaEmbeddings
	rng      *rand.Rand
	nextFile int
	texts    []func() (string, bool)
}

func (cts *cohereTextSource) Document() (string, error) {
	text, err := cts.getText()
	return template.CleanText(text), err
}

func (cts *cohereTextSource) getText() (string, error) {
	for {
		if len(cts.texts) == 0 {
			if err := cts.loadNextFile(context.Background()); err != nil {
				return "", fmt.Errorf("failed to load next file: %w", err)
			}
			continue
		}
		text, ok := cts.texts[0]()
		if !ok {
			cts.texts = cts.texts[1:]
			continue
		}
		return text, nil
	}
}

func (cts *cohereTextSource) loadNextFile(ctx context.Context) error {
	fileIdx := cts.nextFile
	if fileIdx >= len(cohereWikipediaEmbeddingFiles) {
		return errors.New("no more files")
	}
	cts.nextFile++

	var (
		fname = cohereWikipediaEmbeddingFiles[fileIdx]
		fp    = cts.orig.filePath(fname)
	)
	if _, err := os.Stat(fp); os.IsNotExist(err) {
		if err := cts.orig.downloadFile(ctx, fname); err != nil {
			return fmt.Errorf("failed to download file %s: %w", fname, err)
		}
	}

	mmapped, err := template.MemoryMapFile(fp)
	if err != nil {
		return fmt.Errorf("failed to memory map file %s: %w", fp, err)
	}
	textSeq, err := readTextColumn(mmapped.Data, 2)
	if err != nil {
		return fmt.Errorf("failed to read texts from file %s: %w", fp, err)
	}
	pull, _ := iter.Pull(textSeq)
	cts.texts = append(cts.texts, pull)

	return nil
}

func readTextColumn(fileContent []byte, column int64) (iter.Seq[string], error) {
	bf := buffer.NewBufferFileFromBytesNoAlloc(fileContent)
	pr, err := reader.NewParquetColumnReader(bf, 1)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet column reader: %w", err)
	}
	n := pr.GetNumRows()
	texts, _, _, err := pr.ReadColumnByIndex(column, n)
	if err != nil {
		return nil, fmt.Errorf("failed to read column %d: %w", column, err)
	}
	return func(yield func(string) bool) {
		for i := range n {
			if !yield(texts[i].(string)) {
				break
			}
		}
	}, nil
}

var cohereWikipediaEmbeddingFiles = []string{
	"train-00000-of-00253-8d3dffb4e6ef0304.parquet",
	"train-00001-of-00253-2840fd802467fbe7.parquet",
	"train-00002-of-00253-0ecc6c7ff8c4fa3c.parquet",
	"train-00003-of-00253-32f0ed655d4213a4.parquet",
	"train-00004-of-00253-8c33c9247a95d7f8.parquet",
	"train-00005-of-00253-3bddd83c63a94665.parquet",
	"train-00006-of-00253-85d4ae179ff9070e.parquet",
	"train-00007-of-00253-8b41b4aec099f4b5.parquet",
	"train-00008-of-00253-a19dcf6c4aa42b30.parquet",
	"train-00009-of-00253-066c6e7f9bde2198.parquet",
	"train-00010-of-00253-b374cd991cbb5156.parquet",
	"train-00011-of-00253-795bbf4873fcc654.parquet",
	"train-00012-of-00253-4917d4a02ad59415.parquet",
	"train-00013-of-00253-acdc606afcb212c0.parquet",
	"train-00014-of-00253-2aee8194c5707647.parquet",
	"train-00015-of-00253-22d46ba400546eee.parquet",
	"train-00016-of-00253-801956227c7f4aa3.parquet",
	"train-00017-of-00253-6d74f63e2ad1b730.parquet",
	"train-00018-of-00253-255f79226706d5c1.parquet",
	"train-00019-of-00253-f4b2d38fe84c0ead.parquet",
	"train-00020-of-00253-1de85ebaad99658e.parquet",
	"train-00021-of-00253-67ad23e305e3295c.parquet",
	"train-00022-of-00253-f9c5d70b7960cda7.parquet",
	"train-00023-of-00253-b244988e872e8a1d.parquet",
	"train-00024-of-00253-f423c747987257c1.parquet",
	"train-00025-of-00253-29396090fdc2a78a.parquet",
	"train-00026-of-00253-5ddb43fb1770e7f8.parquet",
	"train-00027-of-00253-752a8bc5622d323e.parquet",
	"train-00028-of-00253-da03da3e8033c428.parquet",
	"train-00029-of-00253-5e2ba1902a8f5656.parquet",
	"train-00030-of-00253-c4e515713a5cc8e0.parquet",
	"train-00031-of-00253-5c2c714efc08b13c.parquet",
	"train-00032-of-00253-c9efe2798824ac51.parquet",
	"train-00033-of-00253-be9e14cac59e122e.parquet",
	"train-00034-of-00253-ccc6b075279d9712.parquet",
	"train-00035-of-00253-834ae2f2c5285a99.parquet",
	"train-00036-of-00253-74856ee603b672f0.parquet",
	"train-00037-of-00253-116477598973b86c.parquet",
	"train-00038-of-00253-17b2331379fa72f6.parquet",
	"train-00039-of-00253-7be12594af855ee2.parquet",
	"train-00040-of-00253-84f19228bd41f6da.parquet",
	"train-00041-of-00253-853105c503a5710a.parquet",
	"train-00042-of-00253-f9187b7900752a8a.parquet",
	"train-00043-of-00253-6e57290c3c8b5d5f.parquet",
	"train-00044-of-00253-931ce22b00a4c15b.parquet",
	"train-00045-of-00253-390980d0f55a07cb.parquet",
	"train-00046-of-00253-89ed19ffa8016398.parquet",
	"train-00047-of-00253-3c12f3ead94bfbfb.parquet",
	"train-00048-of-00253-eaa84bcfb85c6f72.parquet",
	"train-00049-of-00253-f60d394df69f0cfd.parquet",
	"train-00050-of-00253-2e9e406b1fe1637d.parquet",
	"train-00051-of-00253-a59dcef64c057c2f.parquet",
	"train-00052-of-00253-c890ab67a7833e35.parquet",
	"train-00053-of-00253-d2066657b608a39c.parquet",
	"train-00054-of-00253-078aebe8abfb2ce8.parquet",
	"train-00055-of-00253-005c356342d5bd48.parquet",
	"train-00056-of-00253-c7aa34ae740fe73c.parquet",
	"train-00057-of-00253-0ac063e31c203212.parquet",
	"train-00058-of-00253-bd2bc91dc377d4a3.parquet",
	"train-00059-of-00253-a49d9bfcc1c73245.parquet",
	"train-00060-of-00253-66bcd667a0f51ca1.parquet",
	"train-00061-of-00253-e03a6c0d915a0d72.parquet",
	"train-00062-of-00253-164bc5605313cf93.parquet",
	"train-00063-of-00253-eb485500a368fb6e.parquet",
	"train-00064-of-00253-0017fc575755acc7.parquet",
	"train-00065-of-00253-8c43415a5f2be2ce.parquet",
	"train-00066-of-00253-ec9c5821e40f26f4.parquet",
	"train-00067-of-00253-dc20a358a4dec4ef.parquet",
	"train-00068-of-00253-a668d48636bd4ad6.parquet",
	"train-00069-of-00253-e5adf7e0505b0ed9.parquet",
	"train-00070-of-00253-a37d9c23f701c52c.parquet",
	"train-00071-of-00253-2ad7eba51e43c84a.parquet",
	"train-00072-of-00253-c750269b7e722e9c.parquet",
	"train-00073-of-00253-306cdafd84214680.parquet",
	"train-00074-of-00253-2d90645be188e613.parquet",
	"train-00075-of-00253-0f2ea04b7339877e.parquet",
	"train-00076-of-00253-93b8a7854df926bd.parquet",
	"train-00077-of-00253-c721bb168a7ab59a.parquet",
	"train-00078-of-00253-ae44665c35f92328.parquet",
	"train-00079-of-00253-c7436cb8e9728f6e.parquet",
	"train-00080-of-00253-49d0d951966b3c22.parquet",
	"train-00081-of-00253-227c70e7b165e2b4.parquet",
	"train-00082-of-00253-1269befa065af101.parquet",
	"train-00083-of-00253-fc5e8a5fa73be0e7.parquet",
	"train-00084-of-00253-f13a198f26475f4b.parquet",
	"train-00085-of-00253-c1fa9e92d40e7c52.parquet",
	"train-00086-of-00253-117382acbaf2d268.parquet",
	"train-00087-of-00253-650b081492b280e8.parquet",
	"train-00088-of-00253-8e74cc842f11c0ca.parquet",
	"train-00089-of-00253-cf36d64831d2fc3a.parquet",
	"train-00090-of-00253-71d852dfdb9d6cfc.parquet",
	"train-00091-of-00253-0912a1fd533b07f1.parquet",
	"train-00092-of-00253-a5c6e71c0c70fec6.parquet",
	"train-00093-of-00253-d3d19c66e736f451.parquet",
	"train-00094-of-00253-890a031231a7fa6b.parquet",
	"train-00095-of-00253-ba267d0930a2f943.parquet",
	"train-00096-of-00253-fd118f187a0a5a70.parquet",
	"train-00097-of-00253-76271b0701e12b92.parquet",
	"train-00098-of-00253-9be1c850b35663be.parquet",
	"train-00099-of-00253-7b61e259ab69e144.parquet",
	"train-00100-of-00253-1a7a4c5d83f9b58d.parquet",
	"train-00101-of-00253-b18b780bfb3cb994.parquet",
	"train-00102-of-00253-8adc6f0687e89f39.parquet",
	"train-00103-of-00253-3f98bb88e6710c42.parquet",
	"train-00104-of-00253-190d8475a05317d6.parquet",
	"train-00105-of-00253-c3783ca560352491.parquet",
	"train-00106-of-00253-805da5014fb3169f.parquet",
	"train-00107-of-00253-f501e794311cc86c.parquet",
	"train-00108-of-00253-7a6399540e7664be.parquet",
	"train-00109-of-00253-6b04d06ed2afe35f.parquet",
	"train-00110-of-00253-fc14df8eb2dba67d.parquet",
	"train-00111-of-00253-35420c7229adc959.parquet",
	"train-00112-of-00253-3ad9687af1fb6db1.parquet",
	"train-00113-of-00253-1b778b3bc5ed1a5a.parquet",
	"train-00114-of-00253-a5caaebba1f2381b.parquet",
	"train-00115-of-00253-c66611cd4369dfea.parquet",
	"train-00116-of-00253-ce5b4f38ffcefe3e.parquet",
	"train-00117-of-00253-6937c17f9c6ee8b0.parquet",
	"train-00118-of-00253-ffd636470e41df94.parquet",
	"train-00119-of-00253-c716b06fe5c720ac.parquet",
	"train-00120-of-00253-950fdfc157360aa5.parquet",
	"train-00121-of-00253-4a433b375723ae25.parquet",
	"train-00122-of-00253-4a048360997b48dc.parquet",
	"train-00123-of-00253-f44a87ba12d3f01f.parquet",
	"train-00124-of-00253-33590ef565c33d3a.parquet",
	"train-00125-of-00253-5d535fbc76c00aff.parquet",
	"train-00126-of-00253-542de0e05c14e36a.parquet",
	"train-00127-of-00253-7caf3e5a3dbd9a93.parquet",
	"train-00128-of-00253-fe0d9efbdafab63d.parquet",
	"train-00129-of-00253-a7d26980242676a1.parquet",
	"train-00130-of-00253-99020d76cab00a44.parquet",
	"train-00131-of-00253-7e616cb3df356909.parquet",
	"train-00132-of-00253-5ff1dae3276d5fd9.parquet",
	"train-00133-of-00253-51dd993b5f02f14f.parquet",
	"train-00134-of-00253-1cc963ff231ae094.parquet",
	"train-00135-of-00253-368cb56b1fcb5abb.parquet",
	"train-00136-of-00253-b7aa50b199c86e5d.parquet",
	"train-00137-of-00253-074847c192f9275c.parquet",
	"train-00138-of-00253-a8a9afd0622163b5.parquet",
	"train-00139-of-00253-5f83fc25ba5044f5.parquet",
	"train-00140-of-00253-66322d24a05da2b9.parquet",
	"train-00141-of-00253-98a7b2c8c1c33319.parquet",
	"train-00142-of-00253-2e7e6803e575bbdc.parquet",
	"train-00143-of-00253-ef5ce0cc0fa39f59.parquet",
	"train-00144-of-00253-476682d833ed9d9a.parquet",
	"train-00145-of-00253-f686dc637743677e.parquet",
	"train-00146-of-00253-622f1f7bac6eb765.parquet",
	"train-00147-of-00253-97c56689522ea998.parquet",
	"train-00148-of-00253-3cf60fddf4af7695.parquet",
	"train-00149-of-00253-fd4e7bc14dffd06f.parquet",
	"train-00150-of-00253-98e0ebf98f324b7f.parquet",
	"train-00151-of-00253-2314099bf6f14c19.parquet",
	"train-00152-of-00253-68218bc90e52b270.parquet",
	"train-00153-of-00253-a96b804645dc1183.parquet",
	"train-00154-of-00253-ce5ffde92833dc3c.parquet",
	"train-00155-of-00253-8842f6af364e4344.parquet",
	"train-00156-of-00253-b2e495e368e3140a.parquet",
	"train-00157-of-00253-fc8d2f720317c51d.parquet",
	"train-00158-of-00253-094972377866b6d7.parquet",
	"train-00159-of-00253-fa1311efd6285c56.parquet",
	"train-00160-of-00253-d481b51d41645f30.parquet",
	"train-00161-of-00253-bb905214b7459ce8.parquet",
	"train-00162-of-00253-8b6842d793b20eb9.parquet",
	"train-00163-of-00253-c217aecaceda2002.parquet",
	"train-00164-of-00253-8f892d491d0426cd.parquet",
	"train-00165-of-00253-b3f683f5ca4ed0bd.parquet",
	"train-00166-of-00253-25f7b96ce2cd2b06.parquet",
	"train-00167-of-00253-53e0c16ecd561461.parquet",
	"train-00168-of-00253-c3b5f215436ca395.parquet",
	"train-00169-of-00253-b2514926fde0539c.parquet",
	"train-00170-of-00253-bf76a82c77844ff7.parquet",
	"train-00171-of-00253-7f3b9c96ce7cd722.parquet",
	"train-00172-of-00253-b8d0406c16d4f34d.parquet",
	"train-00173-of-00253-2ac71b08c877ed93.parquet",
	"train-00174-of-00253-821fceeaf9217d62.parquet",
	"train-00175-of-00253-433c2b9472f3cb6b.parquet",
	"train-00176-of-00253-4ecb0791dff33e14.parquet",
	"train-00177-of-00253-8409a99d82dc08d3.parquet",
	"train-00178-of-00253-8e097439adcc1a8d.parquet",
	"train-00179-of-00253-e9fe011f915f0696.parquet",
	"train-00180-of-00253-7171103b699c1ad2.parquet",
	"train-00181-of-00253-e123f247fd8991e8.parquet",
	"train-00182-of-00253-2a1de2bb55bcf488.parquet",
	"train-00183-of-00253-7a3974aa00c6fe7a.parquet",
	"train-00184-of-00253-6c6df32e5749412b.parquet",
	"train-00185-of-00253-311b99c7bdbb09df.parquet",
	"train-00186-of-00253-b054a3b715d31e45.parquet",
	"train-00187-of-00253-381e78f238e38a05.parquet",
	"train-00188-of-00253-adfaaadc8e8c673e.parquet",
	"train-00189-of-00253-bf26f0488b2a52c7.parquet",
	"train-00190-of-00253-572755709abc79b8.parquet",
	"train-00191-of-00253-654bf58b4a7e741e.parquet",
	"train-00192-of-00253-6603b14592b5c863.parquet",
	"train-00193-of-00253-c7462f0773e54ea5.parquet",
	"train-00194-of-00253-ddd1253bcb446bd2.parquet",
	"train-00195-of-00253-21b71cad8df04442.parquet",
	"train-00196-of-00253-a9fc06012b336c8a.parquet",
	"train-00197-of-00253-149761f60d6f82ce.parquet",
	"train-00198-of-00253-5f84d2689e498ce3.parquet",
	"train-00199-of-00253-6df174ec4afbc754.parquet",
	"train-00200-of-00253-f25cbfbc4ecc46e5.parquet",
	"train-00201-of-00253-c0120a0a641a83e5.parquet",
	"train-00202-of-00253-71fd457b00397688.parquet",
	"train-00203-of-00253-0147f12bab09cb08.parquet",
	"train-00204-of-00253-39a83604836d314f.parquet",
	"train-00205-of-00253-2b14def07f4131d0.parquet",
	"train-00206-of-00253-898a272d08173235.parquet",
	"train-00207-of-00253-c77b10aa2f513766.parquet",
	"train-00208-of-00253-232e02b3b4410b93.parquet",
	"train-00209-of-00253-99d95f12a455e6f9.parquet",
	"train-00210-of-00253-5e1e7f42a0538659.parquet",
	"train-00211-of-00253-9e8789ed7b9d09a0.parquet",
	"train-00212-of-00253-a322bc59c67a8eb7.parquet",
	"train-00213-of-00253-d6cdc38743c7166a.parquet",
	"train-00214-of-00253-db75b992eef7e6f3.parquet",
	"train-00215-of-00253-b10c2c91a0ff0461.parquet",
	"train-00216-of-00253-32fd09d79b4bfcb8.parquet",
	"train-00217-of-00253-09fe8e37142afff0.parquet",
	"train-00218-of-00253-9ba4f606c1f890a7.parquet",
	"train-00219-of-00253-77e5f74f50608c84.parquet",
	"train-00220-of-00253-0f358981f5c4b0ea.parquet",
	"train-00221-of-00253-d63cb1b3f67ca2e3.parquet",
	"train-00222-of-00253-e0ae1cc95eb9162f.parquet",
	"train-00223-of-00253-92b87e0ca46a851e.parquet",
	"train-00224-of-00253-95caa824de31383b.parquet",
	"train-00225-of-00253-f18735143103eb3d.parquet",
	"train-00226-of-00253-9e5c2a122e1ee14c.parquet",
	"train-00227-of-00253-18cd94c647ab72c7.parquet",
	"train-00228-of-00253-67f15d553a91ec1c.parquet",
	"train-00229-of-00253-5fd86b234ddf06c4.parquet",
	"train-00230-of-00253-f769913c0527d080.parquet",
	"train-00231-of-00253-7d929a7d638988f1.parquet",
	"train-00232-of-00253-6d7d44691652d499.parquet",
	"train-00233-of-00253-37c0041e33745541.parquet",
	"train-00234-of-00253-9198599261898de8.parquet",
	"train-00235-of-00253-781e2a384bb1d5f3.parquet",
	"train-00236-of-00253-7520b54396b5716f.parquet",
	"train-00237-of-00253-ac832e864517a5c0.parquet",
	"train-00238-of-00253-228a746b4c50d88a.parquet",
	"train-00239-of-00253-0c922ef3686b8db7.parquet",
	"train-00240-of-00253-ebe435b211e745f8.parquet",
	"train-00241-of-00253-7547d4989a92e648.parquet",
	"train-00242-of-00253-08106b9083591997.parquet",
	"train-00243-of-00253-8ec89bb8403bcfbe.parquet",
	"train-00244-of-00253-45b346edb004bb23.parquet",
	"train-00245-of-00253-40e44253337b5228.parquet",
	"train-00246-of-00253-0a5c5d98e0e009a1.parquet",
	"train-00247-of-00253-1290ad384174b5cb.parquet",
	"train-00248-of-00253-891cf07cd5ff0b86.parquet",
	"train-00249-of-00253-b81c028d5c1ec216.parquet",
	"train-00250-of-00253-f335644d88aa7e77.parquet",
	"train-00251-of-00253-768f2f477249701c.parquet",
	"train-00252-of-00253-6c465b1c097702e9.parquet",
}
