from pydantic import BaseModel, Field, PositiveInt

class Order(BaseModel):
    order_id: str
    user_id: str
    product_id: str
    quantity: PositiveInt = Field(..., description="Quantity must be positive")
    status: str = "PENDING"
