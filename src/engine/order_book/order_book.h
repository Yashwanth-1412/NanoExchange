#pragma once

// Compile-time facade for the two order-book implementations. The selected
// implementation exposes the same MEOrderBook API with no virtual dispatch.
#ifdef NANOEXCHANGE_USE_VECTOR_ORDER_BOOK
#include "src/engine/order_book/order_book_vector.h"
#else
#include "src/engine/order_book/order_book_map.h"
#endif
