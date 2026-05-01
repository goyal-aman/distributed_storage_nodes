followed this guide [here](https://medium.com/@nairouasalaton/introduction-to-tracing-in-go-with-jaeger-opentelemetry-71955c2afa39)

Examples
```
func (u UseCaseImplementation) CreateProduct(ctx context.Context, product Product) (Product, error) {
 ctx, span := tracer.Start(ctx, "CreateProduct")
 defer span.End()
 return u.store.CreateProduct(ctx, product)
}
```